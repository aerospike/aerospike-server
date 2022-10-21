# Build Aerospike Server ".deb" Distribution.

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.vars

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/distrev
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV = $(shell $(DEPTH)/build/version)
OS = $(shell $(DEPTH)/build/os_version)
SIZE = $(shell du -k $(BIN_DIR)/asd | cut -f1)
DEPS = 

comma:= ,
empty:=
space:= $(empty) $(empty)

CONF_VERSION = _systemd
DEPS =

PKG_ARCH = $(shell dpkg-architecture -q DEB_BUILD_ARCH)

ifeq ($(ARCH), x86_64)
    # Breaks deb conventions, but continue using x86_64 for backwards
    # compatibility. Reconsider with next major release.
	COMPAT_ARCH = $(ARCH)
else ifeq ($(ARCH), aarch64)
	COMPAT_ARCH = $(PKG_ARCH)
else
	$(error unhandled arch "$(ARCH)")
endif


DEB = $(PKG)/packages/aerospike-server-$(EDITION)-$(REV).$(OS).$(COMPAT_ARCH).deb

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

	install -pm 644 $(PKG)/deb/conffiles $(BUILD_ROOT)/DEBIAN
ifeq ($(EDITION),community)
	cat $(PKG)/deb/conffiles.telemetry >> $(BUILD_ROOT)/DEBIAN/conffiles
endif
	install -pm 755 $(PKG)/deb/postinst.server $(BUILD_ROOT)/DEBIAN/postinst

	install -pm 755 $(BIN_DIR)/asd $(BUILD_ROOT)/usr/bin/asd
	install -pm 755 $(DEPTH)/tools/bin/asd-coldstart $(BUILD_ROOT)/usr/bin/asd-coldstart
	install -pm 755 $(DEPTH)/as/etc/irqbalance-ban.sh $(BUILD_ROOT)/etc/aerospike/irqbalance-ban.sh
	install -pm 644 $(DEPTH)/as/etc/aerospike$(CONF_VERSION).conf $(BUILD_ROOT)/etc/aerospike/aerospike.conf
	install -pm 644 $(DEPTH)/as/etc/README.sample.conf.md $(BUILD_ROOT)/etc/aerospike/sample/README.md
	install -pm 644 $(DEPTH)/as/etc/aerospike_ssd$(CONF_VERSION).conf $(BUILD_ROOT)/etc/aerospike/sample/aerospike_ssd.conf
	install -pm 644 $(DEPTH)/as/etc/aerospike_mesh$(CONF_VERSION).conf $(BUILD_ROOT)/etc/aerospike/sample/aerospike_mesh.conf

ifeq ($(USE_EE),1)
	echo "/etc/aerospike/features.conf" >> $(BUILD_ROOT)/DEBIAN/conffiles
	install -pm 644 $(EEREPO)/etc/eval_features.conf $(BUILD_ROOT)/etc/aerospike/features.conf
endif

	install -p -D -m 644 $(DEPTH)/as/etc/aerospike.service.head $(BUILD_ROOT)/usr/lib/systemd/system/aerospike.service
  ifeq ($(EDITION),community)
	cat $(DEPTH)/as/etc/aerospike.service.telemetry >> $(BUILD_ROOT)/usr/lib/systemd/system/aerospike.service
  endif
	cat $(DEPTH)/as/etc/aerospike.service.tail >> $(BUILD_ROOT)/usr/lib/systemd/system/aerospike.service
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.tmpfiles $(BUILD_ROOT)/etc/tmpfiles.d/aerospike.conf
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.sysconfig $(BUILD_ROOT)/etc/sysconfig/aerospike
	install -p -D -m 755 $(DEPTH)/as/etc/asd-systemd-helper $(BUILD_ROOT)/usr/bin/asd-systemd-helper
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike.service.d/* $(BUILD_ROOT)/etc/systemd/system/aerospike.service.d

	install -d $(OPT_AS)/doc
ifeq ($(EDITION),community)
	install -pm 644 $(DEPTH)/LICENSE $(OPT_AS)/doc/LICENSE
	install -pm 644 $(DEPTH)/LICENSE-AGPL $(OPT_AS)/doc
	install -pm 644 $(DEPTH)/LICENSE-APACHE $(OPT_AS)/doc
else
	install -pm 644 $(EEREPO)/LICENSE $(OPT_AS)/doc/LICENSE
endif

ifeq ($(EDITION),community)
	install -pm 644 $(DEPTH)/as/etc/aerospike_telemetry.service $(BUILD_ROOT)/usr/lib/systemd/system/aerospike_telemetry.service
	install -pm 644 $(DEPTH)/as/etc/aerospike_telemetry.sysconfig $(BUILD_ROOT)/etc/sysconfig/aerospike_telemetry
	install -d $(OPT_AS)/telemetry
	install -d $(OPT_AS)/telemetry/phonehome
	install -d $(OPT_AS)/telemetry/daemon
	install -pm 644 $(DEPTH)/as/etc/telemetry.conf $(BUILD_ROOT)/etc/aerospike
	install -pm 644 $(DEPTH)/modules/telemetry/README.md $(OPT_AS)/doc/TELEMETRY.md
	install -pm 755 $(DEPTH)/modules/telemetry/telemetry.py $(OPT_AS)/telemetry
	install -pm 644 $(DEPTH)/modules/telemetry/phonehome/*.py $(OPT_AS)/telemetry/phonehome
	install -pm 644 $(DEPTH)/modules/telemetry/daemon/*.py $(OPT_AS)/telemetry/daemon
endif

	install -d $(OPT_AS)/data
	install -d $(OPT_AS)/smd
	install -d $(OPT_AS)/usr/udf/lua

	install -d $(OPT_AS)/bin
	install -pm 755 $(DEPTH)/tools/memacct/asparsemem $(OPT_AS)/bin

	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/deb/server-64 > $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@EDITION@/'$(EDITION)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@ARCH@/'$(PKG_ARCH)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@SIZE@/'$(SIZE)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@DEPS@/$(addprefix $(comma), $(subst $(space),$(comma),$(strip $(DEPS))))/g' $(BUILD_ROOT)/DEBIAN/control

package:
	install -pm 644 $(OPT_AS)/doc/LICENSE $(PKG)/packages
	fakeroot dpkg-deb --build $(BUILD_ROOT) $(DEB)

clean:
	rm -rf $(SOURCE_ROOT)/*

ifeq ($(USE_EE),1)
  include $(AS_EE)/make_in/Makefile.deb.in
endif
