# Aerospike Server
# Makefile
#
# Main Build Targets:
#
#   make {all|server} - Build the Aerospike Server.
#   make clean        - Remove build products, excluding built packages.
#   make cleanpkg     - Remove built packages.
#   make cleanall     - Remove all build products, including built packages.
#   make cleangit     - Remove all files untracked by Git.  (Use with caution!)
#   make strip        - Build stripped versions of the server executables.
#
# Packaging Targets:
#
#   make deb     - Package server for Debian / Ubuntu platforms as a ".deb" file.
#   make rpm     - Package server for the Red Hat Package Manager (RPM.)
#   make tar     - Package server as a compressed tarball for every Linux platform.
#   make source  - Package the server source code as a compressed "tar" archive.
#
# Building a distribution release is a two step process:
#
#   1). The initial "make" builds the server itself.
#
#   2). The second step packages up the server using "make" with one of the following targets:
#
#       rpm:  Suitable for building and installing on Red Hat-derived systems.
#       deb:  Suitable for building and installing on Debian-derived systems.
#       tar:  Makes an "Every Linux" distribution, packaged as a compressed "tar" archive.
#
# Targets for running the Aerospike Server in the source tree:
#
#   make init    - Initialize the server run-time directories.
#   make start   - Start the server.
#   make stop    - Stop the server.
#

OS = $(shell build/os_version)
UNAME=$(shell uname)

# Common variable definitions:
include make_in/Makefile.vars

.PHONY: all server
all server: aslibs
	$(MAKE) -C as OS=$(OS)

.PHONY: lib
lib: aslibs
	$(MAKE) -C as $@ STATIC_LIB=1 OS=$(OS)

.PHONY: aslibs
aslibs: targetdirs version $(JANSSON)/Makefile $(JEMALLOC)/Makefile $(LIBBACKTRACE)/Makefile $(LUAJIT)/src/luaconf.h s2lib
	$(MAKE) -C $(JANSSON)
	$(MAKE) -C $(JEMALLOC)
	$(MAKE) -C $(LIBBACKTRACE)
ifeq ($(USE_LUAJIT),1)
	$(MAKE) -C $(LUAJIT) Q= TARGET_SONAME=libluajit.so CCDEBUG=-g
endif
ifeq ($(ARCH), aarch64)
	$(MAKE) -C $(TSO)
endif
	$(MAKE) -C $(COMMON) CF=$(CF) EXT_CFLAGS="$(EXT_CFLAGS)" OS=$(UNAME)
	$(MAKE) -C $(CF)
	$(MAKE) -C $(MOD_LUA) CF=$(CF) COMMON=$(COMMON) EXT_CFLAGS="$(EXT_CFLAGS)" USE_LUAJIT=$(USE_LUAJIT) LUAJIT=$(LUAJIT) TARGET_SERVER=1 OS=$(UNAME)

S2_FLAGS = -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=RelWithDebInfo

.PHONY: absllib
absllib:
ifeq ($(OS),$(filter $(OS), el7 ubuntu18.04)) # cmake version < 3.13
	mkdir -p $(ABSL)/build
	cd $(ABSL)/build && $(CMAKE) $(S2_FLAGS) -DCMAKE_INSTALL_PREFIX=$(ABSL)/installation -DABSL_ENABLE_INSTALL=ON -DCMAKE_INSTALL_MESSAGE=LAZY -DCMAKE_TARGET_MESSAGES=OFF ..
else
	$(CMAKE) -S $(ABSL) -B $(ABSL)/build $(S2_FLAGS) -DCMAKE_INSTALL_PREFIX=$(ABSL)/installation -DABSL_ENABLE_INSTALL=ON -DCMAKE_INSTALL_MESSAGE=LAZY -DCMAKE_TARGET_MESSAGES=OFF
endif
	$(CMAKE) --build $(ABSL)/build -- --no-print-directory
	$(CMAKE) --build $(ABSL)/build --target install -- --no-print-directory
	ar rcsT $(ABSL_LIB_DIR)/libabsl.a $(ABSL_LIB_DIR)/libabsl_*.a

.PHONY: s2lib
s2lib: absllib
ifeq ($(OS),$(filter $(OS), el7 ubuntu18.04)) # cmake version < 3.13
	mkdir -p $(S2)/build
	cd $(S2)/build && $(CMAKE) $(S2_FLAGS) $(if $(OPENSSL_INCLUDE_DIR),-DOPENSSL_INCLUDE_DIR=$(OPENSSL_INCLUDE_DIR),) -DCMAKE_PREFIX_PATH=$(ABSL)/installation -DBUILD_SHARED_LIBS=OFF ..
else
	$(CMAKE) -S $(S2) -B $(S2)/build $(S2_FLAGS) $(if $(OPENSSL_INCLUDE_DIR),-DOPENSSL_INCLUDE_DIR=$(OPENSSL_INCLUDE_DIR),) -DCMAKE_PREFIX_PATH=$(ABSL)/installation -DBUILD_SHARED_LIBS=OFF
endif
	$(CMAKE) --build $(S2)/build -- -j 8  # Limit threads as the default spawns too many

.PHONY: targetdirs
targetdirs:
	mkdir -p $(GEN_DIR) $(LIBRARY_DIR) $(BIN_DIR)
	mkdir -p $(OBJECT_DIR)/base $(OBJECT_DIR)/fabric \
		$(OBJECT_DIR)/geospatial $(OBJECT_DIR)/query \
		$(OBJECT_DIR)/sindex $(OBJECT_DIR)/storage \
		$(OBJECT_DIR)/transaction $(OBJECT_DIR)/xdr

strip:	server
	$(MAKE) -C as strip

.PHONY: init start stop
init:
	@echo "Creating and initializing working directories..."
	mkdir -p run/log run/work/smd run/work/usr/udf/lua

start:
	@echo "Running the Aerospike Server locally..."
	@PIDFILE=run/asd.pid ; if [ -f $$PIDFILE ]; then echo "Aerospike already running?  Please do \"make stop\" first."; exit -1; fi
	@nohup ./modules/telemetry/telemetry.py as/etc/telemetry_dev.conf > /dev/null 2>&1 &
	$(BIN_DIR)/asd --config-file as/etc/aerospike_dev.conf

stop:
	@echo "Stopping the local Aerospike Server..."
	@PIDFILE=run/asd.pid ; if [ -f $$PIDFILE ]; then kill `cat $$PIDFILE`; rm $$PIDFILE; fi
	@PID=`pgrep telemetry.py | grep -v grep`; if [ -n "$$PID" ]; then kill $$PID; fi

.PHONY: clean
clean:	cleanbasic cleanmodules cleandist

.PHONY: cleanbasic
cleanbasic:
	$(RM) $(VERSION_SRC) $(VERSION_OBJ)
	$(RM) -rf $(TARGET_DIR)
	$(MAKE) -C $(TSO) clean

.PHONY: cleanmodules
cleanmodules:
	$(MAKE) -C $(COMMON) clean
	if [ -e "$(JANSSON)/Makefile" ]; then \
		$(MAKE) -C $(JANSSON) clean; \
		$(MAKE) -C $(JANSSON) distclean; \
	fi
	if [ -e "$(JEMALLOC)/Makefile" ]; then \
		$(MAKE) -C $(JEMALLOC) clean; \
		$(MAKE) -C $(JEMALLOC) distclean; \
	fi
	if [ -e "$(LIBBACKTRACE)/Makefile" ]; then \
		$(MAKE) -C $(LIBBACKTRACE) clean; \
	fi
	if [ -e "$(LUAJIT)/Makefile" ]; then \
		$(MAKE) -C $(LUAJIT) clean; \
	fi
	$(MAKE) -C $(MOD_LUA) COMMON=$(COMMON) USE_LUAJIT=$(USE_LUAJIT) LUAJIT=$(LUAJIT) clean
	$(RM) -rf $(ABSL)/build $(ABSL)/installation # ABSL default clean leaves files in build directory
	$(RM) -rf $(S2)/build # S2 default clean leaves files in build directory

.PHONY: cleandist
cleandist:
	$(RM) -r pkg/dist/*

.PHONY: cleanall
cleanall: clean cleanpkg

.PHONY: cleanpkg
cleanpkg:
	$(RM) pkg/packages/*

GIT_CLEAN = git clean -fdx

.PHONY: cleangit
cleangit:
	cd $(COMMON); $(GIT_CLEAN)
	cd $(JANSSON); $(GIT_CLEAN)
	cd $(JEMALLOC); $(GIT_CLEAN)
	cd $(LIBBACKTRACE); $(GIT_CLEAN)
	cd $(LUAJIT); $(GIT_CLEAN)
	cd $(MOD_LUA); $(GIT_CLEAN)
	cd $(S2); $(GIT_CLEAN)
	$(GIT_CLEAN)

.PHONY: rpm deb tar
rpm deb tar src:
	$(MAKE) -C pkg/$@ EDITION=$(EDITION)

$(VERSION_SRC):	targetdirs
	build/gen_version $(EDITION) $(OS) $(ARCH) $(EE_SHA) > $(VERSION_SRC)

$(VERSION_OBJ):	$(VERSION_SRC)
	$(CC) -o $@ -c $<

.PHONY: version
version:	$(VERSION_OBJ)

$(JANSSON)/configure:
	cd $(JANSSON) && autoreconf -i

$(JANSSON)/Makefile: $(JANSSON)/configure
	cd $(JANSSON) && ./configure $(JANSSON_CONFIG_OPT)

$(JEMALLOC)/configure:
	cd $(JEMALLOC) && autoconf

$(JEMALLOC)/Makefile: $(JEMALLOC)/configure
	cd $(JEMALLOC) && ./configure $(JEM_CONFIG_OPT)

$(LIBBACKTRACE)/Makefile: $(LIBBACKTRACE)/configure
	cd $(LIBBACKTRACE) && ./configure $(LIBBACKTRACE_CONFIG_OPT)

.PHONY: source
source: src

tags etags:
	etags `find as cf modules $(EEREPO) -name "*.[ch]" -o -name "*.cc" | egrep -v '(target/Linux|m4)'` `find /usr/include -name "*.h"`

# Common target definitions:
ifneq ($(EEREPO),)
  include $(EEREPO)/make_in/Makefile.targets
endif
