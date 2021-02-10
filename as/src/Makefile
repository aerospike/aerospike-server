# Aerospike Server
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

# Use SystemTap?  [By default, no.]
USE_SYSTEMTAP = 0

ifeq ($(USE_SYSTEMTAP),1)
CFLAGS +=	-DUSE_SYSTEMTAP
endif

ifeq ($(USE_SYSTEMTAP),1)
SYSTEMTAP_PROBES_D = base/probes.d
SYSTEMTAP_PROBES_H = $(GEN_DIR)/probes.h
SYSTEMTAP_PROBES_O = $(OBJECT_DIR)/probes.o
endif

ifeq ($(USE_EE),1)
  include $(EEREPO)/as/make_in/Makefile.vars
endif

BASE_HEADERS += aggr.h batch.h cdt.h cfg.h datamodel.h exp.h features.h health.h
BASE_HEADERS += index.h job_manager.h json_init.h monitor.h nsup.h particle.h
BASE_HEADERS += particle_blob.h particle_integer.h proto.h scan.h scan_job.h
BASE_HEADERS += scan_manager.h secondary_index.h security.h security_config.h
BASE_HEADERS += service.h smd.h stats.h thr_info.h thr_query.h thr_sindex.h
BASE_HEADERS += thr_tsvc.h ticker.h transaction.h transaction_policy.h
BASE_HEADERS += truncate.h udf_aerospike.h udf_arglist.h udf_cask.h udf_record.h
BASE_HEADERS += xdr.h

BASE_SOURCES += aggr.c as.c batch.c bin.c cdt.c cfg.c exp.c health.c index.c
BASE_SOURCES += job_manager.c json_init.c monitor.c namespace.c nsup.c
BASE_SOURCES += particle.c particle_blob.c particle_bool.c particle_float.c
BASE_SOURCES += particle_geojson.c particle_hll.c particle_integer.c
BASE_SOURCES += particle_list.c particle_map.c particle_string.c predexp.c
BASE_SOURCES += proto.c record.c scan.c scan_job.c scan_manager.c signal.c
BASE_SOURCES += secondary_index.c service.c smd.c thr_info.c thr_info_port.c
BASE_SOURCES += thr_query.c thr_sindex.c thr_tsvc.c ticker.c transaction.c
BASE_SOURCES += truncate.c udf_aerospike.c udf_arglist.c udf_cask.c udf_record.c

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
  BASE_SOURCES += truncate_ce.c
  BASE_SOURCES += xdr_ce.c
endif

FABRIC_HEADERS += clustering.h endpoint.h exchange.h fabric.h hb.h hlc.h
FABRIC_HEADERS += meta_batch.h migrate.h partition.h partition_balance.h
FABRIC_HEADERS += roster.h service_list.h skew_monitor.h

FABRIC_SOURCES += clustering.c endpoint.c exchange.c fabric.c hb.c hlc.c
FABRIC_SOURCES += migrate.c partition.c partition_balance.c service_list.c
FABRIC_SOURCES += skew_monitor.c
ifneq ($(USE_EE),1)
  FABRIC_SOURCES += meta_batch_ce.c
  FABRIC_SOURCES += migrate_ce.c
  FABRIC_SOURCES += partition_balance_ce.c
  FABRIC_SOURCES += partition_ce.c
  FABRIC_SOURCES += roster_ce.c
endif

GEOSPATIAL_HEADERS += geospatial.h
GEOSPATIAL_SOURCES += geospatial.cc geojson.cc

STORAGE_HEADERS += drv_common.h drv_ssd.h flat.h storage.h
STORAGE_SOURCES += drv_common.c drv_memory.c drv_ssd.c flat.c
STORAGE_SOURCES += storage.c
ifneq ($(USE_EE),1)
  STORAGE_SOURCES += drv_memory_ce.c
  STORAGE_SOURCES += drv_pmem_ce.c
  STORAGE_SOURCES += drv_ssd_ce.c
  STORAGE_SOURCES += flat_ce.c
endif

TRANSACTION_HEADERS += delete.h duplicate_resolve.h proxy.h re_replicate.h
TRANSACTION_HEADERS += read.h replica_ping.h replica_write.h rw_request_hash.h
TRANSACTION_HEADERS += rw_request.h rw_utils.h udf.h write.h

TRANSACTION_SOURCES += delete.c duplicate_resolve.c proxy.c read.c
TRANSACTION_SOURCES += replica_write.c rw_request_hash.c rw_request.c rw_utils.c
TRANSACTION_SOURCES += udf.c write.c
ifneq ($(USE_EE),1)
  TRANSACTION_SOURCES += delete_ce.c
  TRANSACTION_SOURCES += re_replicate_ce.c
  TRANSACTION_SOURCES += replica_ping_ce.c
  TRANSACTION_SOURCES += rw_utils_ce.c
endif

HEADERS = $(BASE_HEADERS:%=base/%)
HEADERS += $(FABRIC_HEADERS:%=fabric/%)
HEADERS += $(STORAGE_HEADERS:%=storage/%)
HEADERS += $(GEOSPATIAL_HEADERS:%=geospatial/%)
HEADERS += $(TRANSACTION_HEADERS:%=transaction/%)
HEADERS += $(XDR_HEADERS:%=xdr/%)

SOURCES = $(BASE_SOURCES:%=base/%)
SOURCES += $(FABRIC_SOURCES:%=fabric/%)
SOURCES += $(STORAGE_SOURCES:%=storage/%)
SOURCES += $(GEOSPATIAL_SOURCES:%=geospatial/%)
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

OBJECTS.c = $(SOURCES:%.c=$(OBJECT_DIR)/%.o) $(VERSION_OBJ) $(SYSTEMTAP_PROBES_O)
OBJECTS = $(OBJECTS.c:%.cc=$(OBJECT_DIR)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: $(SYSTEMTAP_PROBES_H) $(SERVER)

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

ifeq ($(USE_SYSTEMTAP),1)
$(SYSTEMTAP_PROBES_H):	$(SYSTEMTAP_PROBES_D)
	dtrace -h -s $< -o $@

$(SYSTEMTAP_PROBES_O):	$(SYSTEMTAP_PROBES_D)
	dtrace -G -s $< -o $@
endif
