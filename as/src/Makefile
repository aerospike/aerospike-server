# Aerospike Server
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

ifeq ($(USE_EE),1)
  include $(EEREPO)/as/make_in/Makefile.vars
endif

BASE_HEADERS += aggr.h
BASE_HEADERS += batch.h
BASE_HEADERS += cdt.h
BASE_HEADERS += cfg.h
BASE_HEADERS += datamodel.h
BASE_HEADERS += exp.h
BASE_HEADERS += expop.h
BASE_HEADERS += features.h
BASE_HEADERS += health.h
BASE_HEADERS += index.h
BASE_HEADERS += json_init.h
BASE_HEADERS += monitor.h
BASE_HEADERS += nsup.h
BASE_HEADERS += particle.h
BASE_HEADERS += particle_blob.h
BASE_HEADERS += particle_integer.h
BASE_HEADERS += proto.h
BASE_HEADERS += scan.h
BASE_HEADERS += scan_job.h
BASE_HEADERS += scan_manager.h
BASE_HEADERS += security.h
BASE_HEADERS += security_config.h
BASE_HEADERS += service.h
BASE_HEADERS += set_index.h
BASE_HEADERS += smd.h
BASE_HEADERS += stats.h
BASE_HEADERS += thr_info.h
BASE_HEADERS += thr_query.h
BASE_HEADERS += thr_tsvc.h
BASE_HEADERS += ticker.h
BASE_HEADERS += transaction.h
BASE_HEADERS += transaction_policy.h
BASE_HEADERS += truncate.h
BASE_HEADERS += udf_aerospike.h
BASE_HEADERS += udf_arglist.h
BASE_HEADERS += udf_cask.h
BASE_HEADERS += udf_record.h
BASE_HEADERS += xdr.h

BASE_SOURCES += aggr.c
BASE_SOURCES += as.c
BASE_SOURCES += batch.c
BASE_SOURCES += bin.c
BASE_SOURCES += cdt.c
BASE_SOURCES += cfg.c
BASE_SOURCES += exp.c
BASE_SOURCES += expop.c
BASE_SOURCES += health.c
BASE_SOURCES += index.c
BASE_SOURCES += json_init.c
BASE_SOURCES += monitor.c
BASE_SOURCES += namespace.c
BASE_SOURCES += nsup.c
BASE_SOURCES += particle.c
BASE_SOURCES += particle_blob.c
BASE_SOURCES += particle_bool.c
BASE_SOURCES += particle_float.c
BASE_SOURCES += particle_geojson.c
BASE_SOURCES += particle_hll.c
BASE_SOURCES += particle_integer.c
BASE_SOURCES += particle_list.c
BASE_SOURCES += particle_map.c
BASE_SOURCES += particle_string.c
BASE_SOURCES += predexp.c
BASE_SOURCES += proto.c
BASE_SOURCES += record.c
BASE_SOURCES += scan.c
BASE_SOURCES += scan_job.c
BASE_SOURCES += scan_manager.c
BASE_SOURCES += service.c
BASE_SOURCES += set_index.c
BASE_SOURCES += signal.c
BASE_SOURCES += smd.c
BASE_SOURCES += thr_info.c
BASE_SOURCES += thr_info_port.c
BASE_SOURCES += thr_query.c
BASE_SOURCES += thr_tsvc.c
BASE_SOURCES += ticker.c
BASE_SOURCES += transaction.c
BASE_SOURCES += truncate.c
BASE_SOURCES += udf_aerospike.c
BASE_SOURCES += udf_arglist.c
BASE_SOURCES += udf_cask.c
BASE_SOURCES += udf_record.c
ifneq ($(STATIC_LIB),1)
  BASE_SOURCES += main.c
endif
ifneq ($(USE_EE),1)
  BASE_SOURCES += bin_ce.c
  BASE_SOURCES += cfg_ce.c
  BASE_SOURCES += features_ce.c
  BASE_SOURCES += index_ce.c
  BASE_SOURCES += namespace_ce.c
  BASE_SOURCES += proto_ce.c
  BASE_SOURCES += record_ce.c
  BASE_SOURCES += security_ce.c
  BASE_SOURCES += set_index_ce.c
  BASE_SOURCES += truncate_ce.c
  BASE_SOURCES += xdr_ce.c
endif

FABRIC_HEADERS += clustering.h
FABRIC_HEADERS += endpoint.h
FABRIC_HEADERS += exchange.h
FABRIC_HEADERS += fabric.h
FABRIC_HEADERS += hb.h
FABRIC_HEADERS += hlc.h
FABRIC_HEADERS += meta_batch.h
FABRIC_HEADERS += migrate.h
FABRIC_HEADERS += partition.h
FABRIC_HEADERS += partition_balance.h
FABRIC_HEADERS += roster.h
FABRIC_HEADERS += service_list.h
FABRIC_HEADERS += skew_monitor.h

FABRIC_SOURCES += clustering.c
FABRIC_SOURCES += endpoint.c
FABRIC_SOURCES += exchange.c
FABRIC_SOURCES += fabric.c
FABRIC_SOURCES += hb.c
FABRIC_SOURCES += hlc.c
FABRIC_SOURCES += migrate.c
FABRIC_SOURCES += partition.c
FABRIC_SOURCES += partition_balance.c
FABRIC_SOURCES += service_list.c
FABRIC_SOURCES += skew_monitor.c
ifneq ($(USE_EE),1)
  FABRIC_SOURCES += meta_batch_ce.c
  FABRIC_SOURCES += migrate_ce.c
  FABRIC_SOURCES += partition_balance_ce.c
  FABRIC_SOURCES += partition_ce.c
  FABRIC_SOURCES += roster_ce.c
endif

GEOSPATIAL_HEADERS += geospatial.h

GEOSPATIAL_SOURCES += geojson.cc
GEOSPATIAL_SOURCES += geospatial.cc

SINDEX_HEADERS += gc.h
SINDEX_HEADERS += populate.h
SINDEX_HEADERS += secondary_index.h

SINDEX_SOURCES += gc.c
SINDEX_SOURCES += populate.c
SINDEX_SOURCES += secondary_index.c
ifneq ($(USE_EE),1)
  SINDEX_SOURCES += gc_ce.c
endif

STORAGE_HEADERS += drv_common.h
STORAGE_HEADERS += drv_ssd.h
STORAGE_HEADERS += flat.h
STORAGE_HEADERS += storage.h

STORAGE_SOURCES += drv_memory.c
STORAGE_SOURCES += drv_ssd.c
STORAGE_SOURCES += flat.c
STORAGE_SOURCES += drv_common.c
STORAGE_SOURCES += storage.c
ifneq ($(USE_EE),1)
  STORAGE_SOURCES += drv_memory_ce.c
  STORAGE_SOURCES += drv_pmem_ce.c
  STORAGE_SOURCES += drv_ssd_ce.c
  STORAGE_SOURCES += flat_ce.c
endif

TRANSACTION_HEADERS += delete.h
TRANSACTION_HEADERS += duplicate_resolve.h
TRANSACTION_HEADERS += proxy.h
TRANSACTION_HEADERS += re_replicate.h
TRANSACTION_HEADERS += read.h
TRANSACTION_HEADERS += replica_ping.h
TRANSACTION_HEADERS += replica_write.h
TRANSACTION_HEADERS += rw_request.h
TRANSACTION_HEADERS += rw_request_hash.h
TRANSACTION_HEADERS += rw_utils.h
TRANSACTION_HEADERS += udf.h
TRANSACTION_HEADERS += write.h

TRANSACTION_SOURCES += delete.c
TRANSACTION_SOURCES += duplicate_resolve.c
TRANSACTION_SOURCES += proxy.c
TRANSACTION_SOURCES += read.c
TRANSACTION_SOURCES += replica_write.c
TRANSACTION_SOURCES += rw_request.c
TRANSACTION_SOURCES += rw_request_hash.c
TRANSACTION_SOURCES += rw_utils.c
TRANSACTION_SOURCES += udf.c
TRANSACTION_SOURCES += write.c
ifneq ($(USE_EE),1)
  TRANSACTION_SOURCES += delete_ce.c
  TRANSACTION_SOURCES += re_replicate_ce.c
  TRANSACTION_SOURCES += replica_ping_ce.c
  TRANSACTION_SOURCES += rw_utils_ce.c
endif

HEADERS = $(BASE_HEADERS:%=base/%)
HEADERS += $(FABRIC_HEADERS:%=fabric/%)
HEADERS += $(GEOSPATIAL_HEADERS:%=geospatial/%)
HEADERS += $(SINDEX_HEADERS:%=sindex/%)
HEADERS += $(STORAGE_HEADERS:%=storage/%)
HEADERS += $(TRANSACTION_HEADERS:%=transaction/%)
HEADERS += $(XDR_HEADERS:%=xdr/%)

SOURCES = $(BASE_SOURCES:%=base/%)
SOURCES += $(FABRIC_SOURCES:%=fabric/%)
SOURCES += $(GEOSPATIAL_SOURCES:%=geospatial/%)
SOURCES += $(SINDEX_SOURCES:%=sindex/%)
SOURCES += $(STORAGE_SOURCES:%=storage/%)
SOURCES += $(TRANSACTION_SOURCES:%=transaction/%)
SOURCES += $(XDR_SOURCES:%=xdr/%)

SERVER = $(BIN_DIR)/asd

INCLUDES += $(INCLUDE_DIR:%=-I%)
INCLUDES += -I$(CF)/include
INCLUDES += -I$(AI)/include
INCLUDES += -I$(COMMON)/target/$(PLATFORM)/include
INCLUDES += -I$(JANSSON)/src
INCLUDES += -I$(MOD_LUA)/target/$(PLATFORM)/include
INCLUDES += -I$(S2)

ifeq ($(USE_LUAJIT),1)
  INCLUDES += -I$(LUAJIT)/src
else
  INCLUDE_LUA_5_1 = /usr/include/lua5.1
  ifneq ($(wildcard $(INCLUDE_LUA_5_1)),)
    INCLUDES += -I$(INCLUDE_LUA_5_1)
    LUA_SUFFIX = 5.1
  endif
endif

AS_LIBRARIES += $(LIBRARY_DIR)/libcf.a
AS_LIBRARIES += $(LIBRARY_DIR)/libai.a
AS_LIBRARIES += $(COMMON)/target/$(PLATFORM)/lib/libaerospike-common.a
AS_LIBRARIES += $(JEMALLOC)/lib/libjemalloc.a
AS_LIBRARIES += $(MOD_LUA)/target/$(PLATFORM)/lib/libmod_lua.a

ifeq ($(DOPROFILE),1)
  LIBRARIES += -pg -fprofile-arcs -lgcov
endif

# Add either the LuaJIT or Lua library
ifeq ($(USE_LUAJIT),1)
  ifeq ($(LD_LUAJIT),static)
    AS_LIBRARIES += $(LUAJIT)/src/libluajit.a
  else
    LIBRARIES += -L$(LUAJIT)/src -lluajit
  endif
else
  ifeq ($(LD_LUA),static)
    # Find and add the static Lua library.
    AS_LIBRARIES += $(or \
      $(wildcard /usr/local/lib/liblua.a), \
      $(wildcard /usr/lib64/liblua$(LUA_SUFFIX).a), \
      $(wildcard /usr/lib/x86_64-linux-gnu/liblua$(LUA_SUFFIX).a), \
      $(wildcard /usr/lib/liblua.a), \
      $(wildcard /usr/lib/powerpc64le-linux-gnu/liblua.a), \
      $(error Cannot find "liblua.a"))
  else
    LIBRARIES += -llua$(LUA_SUFFIX)
  endif
endif

ifeq ($(LD_JANSSON),static)
  AS_LIBRARIES += $(JANSSON)/src/.libs/libjansson.a
else
  LIBRARIES += -L$(JANSSON)/src/.libs -ljansson
endif

LIBRARIES += -L$(S2) -ls2 -ls2cellid -lgoogle-strings -lgoogle-base \
			-lgoogle-util-coding -lgoogle-util-math \
			$(shell curl-config --libs) -lstdc++

LIBRARIES := $(AS_LIBRARIES) $(LIBRARIES)

AS_LIB_DEPS = $(AS_LIBRARIES)

OBJECTS.c = $(SOURCES:%.c=$(OBJECT_DIR)/%.o) $(VERSION_OBJ)
OBJECTS = $(OBJECTS.c:%.cc=$(OBJECT_DIR)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: $(SERVER)

.PHONY: clean
clean:
	$(RM) $(OBJECTS) $(SERVER){,.stripped}
	$(RM) $(DEPENDENCIES)

# Emacs syntax check target.CHK_SOURCES is set by emacs to the files being edited.
.PHONY: check-syntax
check-syntax:
	$(CC) -Wall -Wextra -pedantic -fsyntax-only $(CHK_SOURCES)

.PHONY: lib
lib: $(OBJECTS) $(AS_LIBRARIES)
	ar rcsT $(LIBRARY_DIR)/libaerospike.a $(OBJECTS) $(AS_LIBRARIES)

$(SERVER): $(OBJECTS) $(AS_LIB_DEPS)
	$(LINK.c) -o $(SERVER) $(OBJECTS) $(LIBRARIES)

include $(DEPTH)/make_in/Makefile.targets

# Ignore S2 induced warnings
S2_WNO = -Wno-unused-local-typedefs -Wno-deprecated -Wno-sign-compare
$(OBJECT_DIR)/geospatial/%.o: CXXFLAGS += $(S2_WNO) -D__STDC_LIMIT_MACROS
$(OBJECT_DIR)/geospatial/%.o: CFLAGS := $(filter-out -std=gnu99,$(CFLAGS))
