# Build Aerospike Server ".deb" Distribution.

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.vars

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/distrev
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV := $(shell $(DEPTH)/build/version)
OS := $(shell $(DEPTH)/build/os_version)
BUILD_NUMBER := $(shell $(DEPTH)/build/build_number)
SIZE := $(shell du -k $(BIN_DIR)/asd | cut -f1)
DEPS =

comma:= ,
empty:=
space:= $(empty) $(empty)

PKG_ARCH := $(shell dpkg-architecture -q DEB_BUILD_ARCH)
DEB := $(PKG)/packages/aerospike-server-$(EDITION)_$(REV)-$(BUILD_NUMBER)$(OS)_$(PKG_ARCH).deb

ifeq ($(USE_EE),1)
all:	dist-ee package clean
else
all:	dist package clean
endif

.PHONY: dist
dist:
	install -d $(BUILD_ROOT)/DEBIAN
	install -d $(BUILD_ROOT)/etc/aerospike
	install -d $(BUILD_ROOT)/etc/aerospike/sample
	install -d $(BUILD_ROOT)/usr/lib/systemd/system
	install -d $(BUILD_ROOT)/etc/systemd/system/aerospike.service.d
	install -d $(BUILD_ROOT)/usr/bin
	install -d $(PKG)/deb/debian
	touch $(PKG)/deb/debian/control

	install -pm 644 $(PKG)/deb/conffiles $(BUILD_ROOT)/DEBIAN
	install -pm 755 $(PKG)/deb/postinst.server $(BUILD_ROOT)/DEBIAN/postinst

	install -pm 755 $(BIN_DIR)/asd $(BUILD_ROOT)/usr/bin/asd
	install -pm 755 $(DEPTH)/tools/bin/asd-coldstart $(BUILD_ROOT)/usr/bin/asd-coldstart
	install -pm 755 $(DEPTH)/as/etc/irqbalance-ban.sh $(BUILD_ROOT)/etc/aerospike/irqbalance-ban.sh
	install -pm 644 $(DEPTH)/as/etc/aerospike.conf $(BUILD_ROOT)/etc/aerospike/aerospike.conf
	install -pm 644 $(DEPTH)/as/etc/README.sample.conf.md $(BUILD_ROOT)/etc/aerospike/sample/README.md
	install -pm 644 $(DEPTH)/as/etc/aerospike_ssd.conf $(BUILD_ROOT)/etc/aerospike/sample/aerospike_ssd.conf

ifeq ($(USE_EE),1)
	echo "/etc/aerospike/features.conf" >> $(BUILD_ROOT)/DEBIAN/conffiles
	install -pm 644 $(EEREPO)/etc/eval_features.conf $(BUILD_ROOT)/etc/aerospike/features.conf
endif
ifeq ($(EDITION),federal)
	cat $(EEREPO)/pkg/deb/fips-files >> $(BUILD_ROOT)/DEBIAN/conffiles
endif

	install -p -D -m 644 $(DEPTH)/as/etc/aerospike.service $(BUILD_ROOT)/usr/lib/systemd/system/aerospike.service
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.tmpfiles $(BUILD_ROOT)/etc/tmpfiles.d/aerospike.conf
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.sysconfig $(BUILD_ROOT)/etc/sysconfig/aerospike
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike.service.d/* $(BUILD_ROOT)/etc/systemd/system/aerospike.service.d

	install -d $(OPT_AS)/doc
ifeq ($(EDITION),community)
	install -pm 644 $(DEPTH)/LICENSE $(OPT_AS)/doc/LICENSE
	install -pm 644 $(DEPTH)/LICENSE-AGPL $(OPT_AS)/doc
	install -pm 644 $(DEPTH)/LICENSE-APACHE $(OPT_AS)/doc
else
	install -pm 644 $(EEREPO)/LICENSE $(OPT_AS)/doc/LICENSE
endif

	install -d $(OPT_AS)/data
	install -d $(OPT_AS)/smd
	install -d $(OPT_AS)/usr/udf/lua

	install -d $(OPT_AS)/schema
	install -pm 644 $(SCHEMA_FILE) $(OPT_AS)/schema/aerospike_config_schema.json

	install -d $(OPT_AS)/bin
	install -pm 755 $(DEPTH)/tools/memacct/asparsemem $(OPT_AS)/bin

	sed 's/@VERSION@-1/'$(REV)'-'$(BUILD_NUMBER)'/g' < $(PKG)/deb/server-64 > $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@EDITION@/'$(EDITION)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@ARCH@/'$(PKG_ARCH)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@SIZE@/'$(SIZE)'/g' $(BUILD_ROOT)/DEBIAN/control

.PHONY: gen-deps
gen-deps:
	$(eval DEPS := $(shell /usr/bin/dpkg-shlibdeps -O $(BUILD_ROOT)/usr/bin/asd | sed 's/shlibs:Depends=//'))
	sed -i 's/@DEPS@/$(DEPS)/g' $(BUILD_ROOT)/DEBIAN/control

package: gen-deps
	install -pm 644 $(OPT_AS)/doc/LICENSE $(PKG)/packages
	fakeroot dpkg-deb -Z xz --build $(BUILD_ROOT) $(DEB)

clean:
	rm -rf $(SOURCE_ROOT)/*

ifeq ($(USE_EE),1)
   include $(AS_EE)/make_in/Makefile.deb.in
endif
