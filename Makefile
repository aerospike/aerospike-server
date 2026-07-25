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
# Test Targets:
#
#   make tests              - Build all tests. Depends on EE repo.
#   make run-tests          - Build and run all tests. Depends on EE repo.
#
# Packaging Targets:
#
#   make deb     - Package server for Debian / Ubuntu platforms as a ".deb" file.
#   make rpm     - Package server for the Red Hat Package Manager (RPM.)
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
#

# Common variable definitions:
include make_in/Makefile.vars

.PHONY: all server
all server: aslibs
	$(MAKE) -C as OS=$(OS)

.PHONY: lib
lib: aslibs
	$(MAKE) -C as $@ STATIC_LIB=1 OS=$(OS)

.PHONY: aslibs
aslibs: targetdirs version $(JANSSON)/Makefile $(JEMALLOC)/Makefile $(LIBBACKTRACE)/Makefile s2lib jsonlib jsonschema yamlcpp iculib pcre2lib
	$(MAKE) -C $(JANSSON)
	$(MAKE) -C $(JEMALLOC)
	$(MAKE) -C $(LIBBACKTRACE)
ifeq ($(ARCH), aarch64)
	$(MAKE) -C $(TSO)
endif
	$(MAKE) -C $(COMMON) CF=$(CF) EXT_CFLAGS="$(EXT_CFLAGS)" OS=$(UNAME)
	$(MAKE) -C $(CF)
	$(MAKE) -C $(MOD_LUA) CF=$(CF) COMMON=$(COMMON) LUAMOD=$(LUAMOD) EXT_CFLAGS="$(EXT_CFLAGS)" TARGET_SERVER=1 OS=$(UNAME)

S2_FLAGS = -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=RelWithDebInfo
JSON_SCHEMA_FLAGS = -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_PREFIX_PATH=$(JSON_PATH)/installation

.PHONY: absllib
absllib:
	$(CMAKE) -S $(ABSL) -B $(ABSL)/build -G 'Unix Makefiles' $(S2_FLAGS) -DCMAKE_INSTALL_PREFIX=$(ABSL)/installation -DABSL_ENABLE_INSTALL=ON -DCMAKE_INSTALL_MESSAGE=LAZY -DCMAKE_TARGET_MESSAGES=OFF
	$(MAKE) -C $(ABSL)/build
	$(MAKE) -C $(ABSL)/build install
	ar rcsT $(ABSL_LIB_DIR)/libabsl.a $(ABSL_LIB_DIR)/libabsl_*.a

.PHONY: s2lib
s2lib: absllib
	$(CMAKE) -S $(S2) -B $(S2)/build -G 'Unix Makefiles' $(S2_FLAGS) $(if $(OPENSSL_INCLUDE_DIR),-DOPENSSL_INCLUDE_DIR=$(OPENSSL_INCLUDE_DIR),) -DCMAKE_PREFIX_PATH=$(ABSL)/installation -DBUILD_SHARED_LIBS=OFF -DBUILD_EXAMPLES=OFF
	$(MAKE) -C $(S2)/build

.PHONY: jsonlib
jsonlib:
	# Build nlohmann-json as a CMake project so it creates proper targets
	$(CMAKE) -S $(JSON_PATH) -B $(JSON_PATH)/build -G 'Unix Makefiles' -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DCMAKE_INSTALL_PREFIX=$(JSON_PATH)/installation -DJSON_Install=ON -DJSON_BuildTests=OFF
	$(MAKE) -C $(JSON_PATH)/build install

# NOTE this builds and links a static lib
.PHONY: jsonschema
jsonschema: jsonlib
	# Configure the build using CMake.
	# -S specifies the source directory.
	# -B specifies the build directory (we create one inside the module).
	# -DCMAKE_PREFIX_PATH tells it where to find the nlohmann-json installation.
	$(CMAKE) -S $(JSON_SCHEMA_PATH) -B $(JSON_SCHEMA_PATH)/build -G 'Unix Makefiles' $(JSON_SCHEMA_FLAGS) \
		-DCMAKE_PREFIX_PATH=$(JSON_PATH)/installation -DCMAKE_INSTALL_PREFIX=$(JSON_SCHEMA_PATH)/installation -DBUILD_SHARED_LIBS=OFF
	$(MAKE) -C $(JSON_SCHEMA_PATH)/build install

# NOTE this builds and links a static lib
.PHONY: yamlcpp
yamlcpp:
	# Build yaml-cpp as a CMake project so it creates proper targets
	$(CMAKE) -S $(YAML_CPP_PATH) -B $(YAML_CPP_PATH)/build -G 'Unix Makefiles' \
		-DCMAKE_INSTALL_PREFIX=$(YAML_CPP_PATH)/installation
	$(MAKE) -C $(YAML_CPP_PATH)/build

# ICU4C configure options for static build
# Note: tools are enabled as they're needed to build data files
ICU_CONFIG_OPT = --enable-static --disable-shared --disable-tests --disable-samples \
	--disable-extras --disable-icuio --disable-layoutex \
	--with-data-packaging=static --prefix=$(ICU)/installation

# Subset ICU locale data (root only) and drop unused feature packages; see
# make_in/icu_data_filter.json and particle_string.c (root collator/case map).
ICU_DATA_FILTER_JSON := $(abspath $(DEPTH)/make_in/icu_data_filter.json)

# Sentinel file to ensure configure runs only once with parallel make
$(ICU)/source/.configured: $(ICU_DATA_FILTER_JSON)
	cd $(ICU)/source && ICU_DATA_FILTER_FILE=$(ICU_DATA_FILTER_JSON) ./configure $(ICU_CONFIG_OPT)
	touch $@

.PHONY: iculib
iculib: $(ICU)/source/.configured
	$(MAKE) -C $(ICU)/source
	$(MAKE) -C $(ICU)/source install

.PHONY: pcre2lib
pcre2lib:
	git -C $(PCRE2) submodule update --init
	$(CMAKE) -S $(PCRE2) -B $(PCRE2)/build -G 'Unix Makefiles' \
		-DCMAKE_INSTALL_PREFIX=$(PCRE2)/installation \
		-DCMAKE_INSTALL_LIBDIR=lib \
		-DPCRE2_SUPPORT_JIT=ON -DPCRE2_BUILD_PCRE2GREP=OFF \
		-DPCRE2_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF
	$(MAKE) -C $(PCRE2)/build install

.PHONY: targetdirs
targetdirs:
	mkdir -p $(GEN_DIR) $(LIBRARY_DIR) $(BIN_DIR)
	mkdir -p $(OBJECT_DIR)/base $(OBJECT_DIR)/exp \
		$(OBJECT_DIR)/fabric \
		$(OBJECT_DIR)/geospatial $(OBJECT_DIR)/query \
		$(OBJECT_DIR)/sindex $(OBJECT_DIR)/storage \
		$(OBJECT_DIR)/transaction $(OBJECT_DIR)/xdr

strip:	server
	$(MAKE) -C as strip

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
	if [ -e "$(JSON_PATH)/Makefile" ]; then \
		$(MAKE) -C $(JSON_PATH) clean; \
	fi
	$(RM) -rf $(JSON_PATH)/build $(JSON_PATH)/installation # Clean nlohmann-json CMake build artifacts
	if [ -e "$(JSON_SCHEMA_PATH)/build/Makefile" ]; then \
		$(MAKE) -C $(JSON_SCHEMA_PATH)/build clean; \
	fi
	if [ -e "$(YAML_CPP_PATH)/build/Makefile" ]; then \
		$(MAKE) -C $(YAML_CPP_PATH)/build clean; \
	fi
	if [ -e "$(JEMALLOC)/Makefile" ]; then \
		$(MAKE) -C $(JEMALLOC) clean; \
		$(MAKE) -C $(JEMALLOC) distclean; \
	fi
	if [ -e "$(LIBBACKTRACE)/Makefile" ]; then \
		$(MAKE) -C $(LIBBACKTRACE) clean; \
		$(MAKE) -C $(LIBBACKTRACE) distclean; \
	fi
	$(MAKE) -C $(MOD_LUA) COMMON=$(COMMON) LUAMOD=$(LUAMOD) clean
	$(RM) -rf $(ABSL)/build $(ABSL)/installation # ABSL default clean leaves files in build directory
	$(RM) -rf $(S2)/build # S2 default clean leaves files in build directory
	if [ -e "$(ICU)/source/Makefile" ]; then \
		$(MAKE) -C $(ICU)/source clean; \
		$(MAKE) -C $(ICU)/source distclean; \
	fi
	$(RM) -rf $(ICU)/installation $(ICU)/source/.configured
	$(RM) -rf $(PCRE2)/build $(PCRE2)/installation

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
	cd $(MOD_LUA); $(GIT_CLEAN)
	cd $(S2); $(GIT_CLEAN)
	cd $(JSON_SCHEMA_PATH); $(GIT_CLEAN)
	cd $(JSON_PATH); $(GIT_CLEAN)
	cd $(ICU); $(GIT_CLEAN)
	cd $(PCRE2); $(GIT_CLEAN)
	$(GIT_CLEAN)

.PHONY: rpm deb
rpm deb src: server
	$(MAKE) -C pkg/$@ EDITION=$(EDITION)

$(VERSION_SRC):	targetdirs
	build/gen_version $(EDITION) $(OS) $(ARCH) $(EE_SHA) $(FIPS_SHA) > $(VERSION_SRC)

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

