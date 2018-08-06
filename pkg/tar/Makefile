# Build Aerospike TAR distribution.

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.vars

PKG = $(realpath $(DEPTH)/pkg)
SOURCE = $(PKG)/tar
SOURCE_ROOT = $(PKG)/dist
BUILD_ROOT = $(SOURCE_ROOT)/BUILD
TARGET = $(BUILD_ROOT)/aerospike-server

REV = $(shell $(DEPTH)/build/version)

ARCHIVE = $(PKG)/packages/aerospike-server-$(EDITION)-$(REV).tar

all:	dist package clean

.PHONY: dist
dist:
	@# create directory
	install -pm 755 -Dd $(TARGET)

	@# docs
	install -pm 644 -D $(SOURCE)/README $(TARGET)/README
	install -pm 644 -D $(DEPTH)/LICENSE $(TARGET)/LICENSE

	@# binaries
	install -pm 755 -Dd $(TARGET)/bin
	install -pm 755 -D $(BIN_DIR)/asd $(TARGET)/bin/asd
	install -pm 755 -D $(SOURCE)/bin/aerospike $(TARGET)/bin/aerospike

	@# share binaries
	install -pm 755 -Dd $(TARGET)/share/bin
	install -pm 755 -D $(SOURCE)/share/bin/aerospike $(TARGET)/share/bin/aerospike

	@# share libraries
	install -pm 755 -Dd $(TARGET)/share/lib
	install -pm 644 -D $(SOURCE)/share/lib/* $(TARGET)/share/lib/.

	@# share libexecs
	install -pm 755 -Dd $(TARGET)/share/libexec
	install -pm 644 -D $(SOURCE)/share/libexec/* $(TARGET)/share/libexec/.

	@# share manpages
	install -pm 755 -Dd $(TARGET)/share/man
	install -pm 644 -D $(SOURCE)/share/man/* $(TARGET)/share/man/.

	@# share configs
	install -pm 755 -Dd $(TARGET)/share/etc
	install -pm 644 -D $(SOURCE)/share/etc/aerospike.conf $(TARGET)/share/etc/aerospike.conf
	install -pm 755 -D $(DEPTH)/as/etc/irqbalance-ban.sh $(TARGET)/share/etc/irqbalance-ban.sh

	@# lua files
	install -pm 755 -Dd $(TARGET)/share/udf/lua

tar:
	tar cvf $(ARCHIVE) -C $(BUILD_ROOT) aerospike-server

gzip:	tar
	gzip -f $(ARCHIVE)

package:	gzip

clean:
	rm -rf $(SOURCE_ROOT)/*
