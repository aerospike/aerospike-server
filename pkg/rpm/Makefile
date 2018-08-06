# Build Aerospike Server RPM Distribution.

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.vars

PKG = $(realpath $(DEPTH)/pkg)
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
OPT_AS = $(BUILD_ROOT)/opt/aerospike

REV = $(shell $(DEPTH)/build/version | sed 's/-/_/g')
OS = $(shell $(DEPTH)/build/os_version)

ifeq ($(OS),el7)
  USE_SYSTEMD = 1
  CONF_VERSION = _systemd
endif

ifeq ($(USE_EE),1)
all:	dist-ee package clean
else
all:	dist package clean
endif

.PHONY:dist
dist:
	install -d $(BUILD_ROOT)/etc/aerospike
	install -d $(BUILD_ROOT)/etc/aerospike/sample

ifeq ($(USE_SYSTEMD),1)
	install -d $(BUILD_ROOT)/usr/lib/systemd/system
	install -d $(BUILD_ROOT)/etc/systemd/system/aerospike.service.d
else
	install -d $(BUILD_ROOT)/var/log/aerospike
	install -d $(BUILD_ROOT)/var/run/aerospike
	install -d $(BUILD_ROOT)/etc/init.d
	install -d $(BUILD_ROOT)/etc/logrotate.d
endif
	install -d $(BUILD_ROOT)/usr/bin

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
	cat $(DEPTH)/as/etc/aerospike.service.head >> $(PKG)/rpm/aerospike.service
  ifeq ($(EDITION),community)
	cat $(DEPTH)/as/etc/aerospike.service.telemetry >> $(PKG)/rpm/aerospike.service
  endif
	cat $(DEPTH)/as/etc/aerospike.service.tail >> $(PKG)/rpm/aerospike.service
	install -p -D -m 644 $(PKG)/rpm/aerospike.service $(BUILD_ROOT)/usr/lib/systemd/system/aerospike.service
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.tmpfiles $(BUILD_ROOT)/etc/tmpfiles.d/aerospike.conf
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike-server.sysconfig $(BUILD_ROOT)/etc/sysconfig/aerospike
	install -p -D -m 755 $(DEPTH)/as/etc/asd-systemd-helper $(BUILD_ROOT)/usr/bin/asd-systemd-helper
	install -p -D -m 644 $(DEPTH)/as/etc/aerospike.service.d/* $(BUILD_ROOT)/etc/systemd/system/aerospike.service.d
else
	install -pm 755 $(DEPTH)/as/etc/init-script $(BUILD_ROOT)/etc/init.d/aerospike
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
	install -pm 755 $(DEPTH)/as/etc/init-telemetry-script $(BUILD_ROOT)/etc/init.d/aerospike_telemetry
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

package:
	install -pm 644 $(OPT_AS)/doc/LICENSE $(PKG)/packages
	install -d $(SOURCE_ROOT)/RPMS/x86_64

	sed 's/@VERSION@/'$(REV)'/g' < $(PKG)/rpm/server-spec-base > $(PKG)/rpm/aerospike.spec
ifneq ($(USE_SYSTEMD),1)
	cat $(PKG)/rpm/server-spec-logrotate >> $(PKG)/rpm/aerospike.spec
endif
	cat $(PKG)/rpm/server-spec-files >> $(PKG)/rpm/aerospike.spec
ifeq ($(USE_SYSTEMD),1)
	cat $(PKG)/rpm/server-spec-systemd >> $(PKG)/rpm/aerospike.spec
else
	cat $(PKG)/rpm/server-spec-sysv >> $(PKG)/rpm/aerospike.spec
endif

ifeq ($(USE_EE),1)
	cat $(EEREPO)/pkg/rpm/as-files >> $(PKG)/rpm/aerospike.spec
	cat $(EEREPO)/pkg/rpm/xdr-files >> $(PKG)/rpm/aerospike.spec
endif
	cat $(PKG)/rpm/server-spec-config >> $(PKG)/rpm/aerospike.spec
ifeq ($(EDITION),community)
	cat $(PKG)/rpm/server-spec-telemetry >> $(PKG)/rpm/aerospike.spec
  ifeq ($(USE_SYSTEMD),1)
	cat $(PKG)/rpm/server-spec-telemetry-systemd >> $(PKG)/rpm/aerospike.spec
  else
	cat $(PKG)/rpm/server-spec-telemetry-sysv >> $(PKG)/rpm/aerospike.spec
  endif
endif
	cat $(PKG)/rpm/server-spec-scripts >> $(PKG)/rpm/aerospike.spec
ifeq ($(USE_SYSTEMD),1)
	cat $(PKG)/rpm/server-spec-scripts-systemd >> $(PKG)/rpm/aerospike.spec
endif

	sed -i 's/@RELEASE@/'$(OS)'/g' $(PKG)/rpm/aerospike.spec
	sed -i 's/@EDITION@/'$(EDITION)'/g' $(PKG)/rpm/aerospike.spec

	cd $(DEPTH); rpmbuild -bb -vv --define "dist .$(OS)" --buildroot $(BUILD_ROOT) $(PKG)/rpm/aerospike.spec

	find $(SOURCE_ROOT)/RPMS -type f -exec mv {} $(PKG)/packages \;

clean:
	rm -rf $(PKG)/rpm/aerospike.spec
ifeq ($(USE_SYSTEMD),1)
	rm -rf $(PKG)/rpm/aerospike.service
endif
	rm -rf $(SOURCE_ROOT)/*

ifeq ($(USE_EE),1)
  include $(AS_EE)/make_in/Makefile.rpm.in
  include $(XDR)/make_in/Makefile.rpm.in
endif
