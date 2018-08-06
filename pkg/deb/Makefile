# Build Aerospike Server ".deb" Distribution.

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.vars

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV = $(shell $(DEPTH)/build/version)
OS = $(shell $(DEPTH)/build/os_version)
SIZE = $(shell du -k $(BIN_DIR)/asd | cut -f1)
DEPS = 

comma:= ,
empty:=
space:= $(empty) $(empty)

ifeq ($(OS),$(filter $(OS),debian8 debian9 ubuntu16.04 ubuntu18.04))
	USE_SYSTEMD = 1
	CONF_VERSION = _systemd
	DEPS = 
else
	DEPS = logrotate
endif

ifeq ($(EDITION),community)
  DEPS += python
endif

DEB = $(PKG)/packages/aerospike-server-$(EDITION)-$(REV).$(OS).x86_64.deb

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
ifeq ($(USE_SYSTEMD),1)
	install -d $(BUILD_ROOT)/usr/lib/systemd/system
	install -d $(BUILD_ROOT)/etc/systemd/system/aerospike.service.d
else
	install -d $(BUILD_ROOT)/etc/init.d
	install -d $(BUILD_ROOT)/etc/logrotate.d
	install -d $(BUILD_ROOT)/var/log/aerospike
	install -d $(BUILD_ROOT)/var/run/aerospike
endif
	install -d $(BUILD_ROOT)/usr/bin

	install -pm 644 $(PKG)/deb/conffiles $(BUILD_ROOT)/DEBIAN
ifeq ($(EDITION),community)
	cat $(PKG)/deb/conffiles.telemetry >> $(BUILD_ROOT)/DEBIAN/conffiles
endif
	install -pm 755 $(PKG)/deb/postinst.server $(BUILD_ROOT)/DEBIAN/postinst

	install -pm 755 $(BIN_DIR)/asd $(BUILD_ROOT)/usr/bin/asd
ifeq ($(USE_SYSTEMD),1)
	install -pm 755 $(DEPTH)/tools/bin/asd-coldstart $(BUILD_ROOT)/usr/bin/asd-coldstart
endif
	install -pm 755 $(DEPTH)/tools/citrus2aero/upgrade2to3 $(BUILD_ROOT)/usr/bin/asmigrate2to3
	install -pm 755 $(DEPTH)/tools/fixownership/fixownership.py $(BUILD_ROOT)/usr/bin/asfixownership
	install -pm 755 $(DEPTH)/as/etc/irqbalance-ban.sh $(BUILD_ROOT)/etc/aerospike/irqbalance-ban.sh
	install -pm 644 $(DEPTH)/as/etc/aerospike$(CONF_VERSION).conf $(BUILD_ROOT)/etc/aerospike/aerospike.conf
	cat $(DEPTH)/as/etc/README.sample.conf.md > $(BUILD_ROOT)/etc/aerospike/sample/README.md
	install -pm 644 $(DEPTH)/as/etc/aerospike_ssd$(CONF_VERSION).conf $(BUILD_ROOT)/etc/aerospike/sample/aerospike_ssd.conf
	install -pm 644 $(DEPTH)/as/etc/aerospike_mesh$(CONF_VERSION).conf $(BUILD_ROOT)/etc/aerospike/sample/aerospike_mesh.conf

ifeq ($(USE_SYSTEMD),1)
	cat $(DEPTH)/as/etc/aerospike.service.head >> $(PKG)/deb/aerospike.service
  ifeq ($(EDITION),community)
	cat $(DEPTH)/as/etc/aerospike.service.telemetry >> $(PKG)/deb/aerospike.service
  endif
	cat $(DEPTH)/as/etc/aerospike.service.tail >> $(PKG)/deb/aerospike.service
	install -p -D -m 644 $(PKG)/deb/aerospike.service $(BUILD_ROOT)/usr/lib/systemd/system/aerospike.service
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.tmpfiles $(BUILD_ROOT)/etc/tmpfiles.d/aerospike.conf
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.sysconfig $(BUILD_ROOT)/etc/sysconfig/aerospike
	install -p -D -m 755 $(DEPTH)/as/etc/asd-systemd-helper $(BUILD_ROOT)/usr/bin/asd-systemd-helper
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike.service.d/* $(BUILD_ROOT)/etc/systemd/system/aerospike.service.d
else
	install -pm 755 $(DEPTH)/as/etc/init-script.deb $(BUILD_ROOT)/etc/init.d/aerospike
	sed -i 's/@EDITION@/$(EDITION)/g' $(BUILD_ROOT)/etc/init.d/aerospike
	install -pm 644 $(DEPTH)/as/etc/logrotate_asd $(BUILD_ROOT)/etc/logrotate.d/aerospike
endif

	install -d $(OPT_AS)/doc
ifeq ($(EDITION),community)
	install -pm 644 $(DEPTH)/LICENSE.CE $(OPT_AS)/doc/LICENSE
	install -pm 644 $(DEPTH)/LICENSE-AGPL $(OPT_AS)/doc
	install -pm 644 $(DEPTH)/LICENSE-APACHE $(OPT_AS)/doc
else
	install -pm 644 $(EEREPO)/LICENSE.EE $(OPT_AS)/doc/LICENSE
endif
	cat $(DEPTH)/LICENSE.3rdParty >> $(OPT_AS)/doc/LICENSE

ifeq ($(EDITION),community)
  ifeq ($(USE_SYSTEMD),1)
	install -pm 755 $(DEPTH)/as/etc/aerospike_telemetry.service $(BUILD_ROOT)/usr/lib/systemd/system/aerospike_telemetry.service
	install -pm 644 $(DEPTH)/as/etc/aerospike_telemetry.sysconfig $(BUILD_ROOT)/etc/sysconfig/aerospike_telemetry
  else
	install -pm 755 $(DEPTH)/as/etc/init-telemetry-script.deb $(BUILD_ROOT)/etc/init.d/aerospike_telemetry
	install -pm 644 $(DEPTH)/as/etc/logrotate_telemetry $(BUILD_ROOT)/etc/logrotate.d/aerospike_telemetry
  endif
	install -d $(OPT_AS)/telemetry
	install -d $(OPT_AS)/telemetry/phonehome
	install -d $(OPT_AS)/telemetry/daemon
	install -pm 644 $(DEPTH)/as/etc/telemetry.conf $(BUILD_ROOT)/etc/aerospike
	install -pm 644 $(DEPTH)/modules/telemetry/README.md $(OPT_AS)/doc/TELEMETRY.md
	install -pm 755 $(DEPTH)/modules/telemetry/telemetry.py $(OPT_AS)/telemetry
	install -pm 755 $(DEPTH)/modules/telemetry/phonehome/*.py $(OPT_AS)/telemetry/phonehome
	install -pm 755 $(DEPTH)/modules/telemetry/daemon/*.py $(OPT_AS)/telemetry/daemon
endif

	install -d $(OPT_AS)/data
	install -d $(OPT_AS)/smd
	install -d $(OPT_AS)/usr/udf/lua

	install -d $(OPT_AS)/bin
	install -pm 755 $(DEPTH)/tools/memacct/asparsemem $(OPT_AS)/bin

	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/deb/server-64 > $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@EDITION@/'$(EDITION)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@SIZE@/'$(SIZE)'/g' $(BUILD_ROOT)/DEBIAN/control
	sed -i 's/@DEPS@/$(addprefix $(comma), $(subst $(space),$(comma),$(strip $(DEPS))))/g' $(BUILD_ROOT)/DEBIAN/control

package:
	install -pm 644 $(OPT_AS)/doc/LICENSE $(PKG)/packages
	fakeroot dpkg-deb --build $(BUILD_ROOT) $(DEB)

clean:
	rm -rf $(SOURCE_ROOT)/*
ifeq ($(USE_SYSTEMD),1)
	rm -rf $(PKG)/deb/aerospike.service
endif

ifeq ($(USE_EE),1)
  include $(AS_EE)/make_in/Makefile.deb.in
  include $(XDR)/make_in/Makefile.deb.in
endif
