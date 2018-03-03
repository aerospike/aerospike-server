# Build Aerospike source distribution.

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.vars

REPO=$(realpath $(DEPTH))
PKG = $(REPO)/pkg
DIST = $(PKG)/dist
SOURCE = $(DIST)/SOURCE
SOURCE_ASD = $(SOURCE)/aerospike-server

REV = $(shell $(DEPTH)/build/version)

# Name of the source package:
ARCHIVE = $(DEPTH)/pkg/packages/aerospike-server-$(EDITION)-$(REV).src.tar.bz2

all: dist package clean


.PHONY:dist
dist:
	bash git-cp-files.sh $(REPO) $(SOURCE_ASD)

.PHONY: package
package: dist
	tar cvfj $(ARCHIVE) -C $(SOURCE) aerospike-server

clean:
	rm -rf $(SOURCE)