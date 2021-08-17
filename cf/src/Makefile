# Citrusleaf Foundation
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

ifeq ($(USE_EE),1)
  include $(EEREPO)/cf/make_in/Makefile.vars
endif

HEADERS += arenax.h
HEADERS += bits.h
HEADERS += cf_mutex.h
HEADERS += cf_str.h
HEADERS += cf_thread.h
HEADERS += compare.h
HEADERS += daemon.h
HEADERS += dns.h
HEADERS += dynbuf.h
HEADERS += enhanced_alloc.h
HEADERS += epoll_queue.h
HEADERS += fetch.h
HEADERS += hist.h
HEADERS += linear_hist.h
HEADERS += log.h
HEADERS += msg.h
HEADERS += msgpack_in.h
HEADERS += node.h
HEADERS += os.h
HEADERS += pool.h
HEADERS += rchash.h
HEADERS += shash.h
HEADERS += socket.h
HEADERS += tls.h
HEADERS += vault.h
HEADERS += vector.h
HEADERS += vmapx.h
HEADERS += xmem.h

SOURCES += alloc.c
SOURCES += arenax.c
SOURCES += cf_mutex.c
SOURCES += cf_str.c
SOURCES += cf_thread.c
SOURCES += daemon.c
SOURCES += dns.c
SOURCES += dynbuf.c
SOURCES += epoll_queue.c
SOURCES += fetch.c
SOURCES += hardware.c
SOURCES += hist.c
SOURCES += linear_hist.c
SOURCES += log.c
SOURCES += msg.c
SOURCES += msgpack_in.c
SOURCES += node.c
SOURCES += os.c
SOURCES += pool.c
SOURCES += rchash.c
SOURCES += shash.c
SOURCES += socket.c
SOURCES += vector.c
SOURCES += vmapx.c

ifneq ($(USE_EE),1)
  SOURCES += arenax_ce.c
  SOURCES += socket_ce.c
  SOURCES += tls_ce.c
  SOURCES += vault_ce.c
endif

LIBRARY = $(LIBRARY_DIR)/libcf.a

INCLUDES += $(INCLUDE_DIR:%=-I%)
INCLUDES += -I$(COMMON)/src/include
INCLUDES += -I$(JANSSON)/src
INCLUDES += -I$(JEMALLOC)/include

OBJECTS = $(SOURCES:%.c=$(OBJECT_DIR)/%.o)
DEPENDENCIES = $(OBJECTS:%.o=%.d)

.PHONY: all
all: $(LIBRARY)

.PHONY: clean
clean:
	$(RM) $(OBJECTS) $(LIBRARY)
	$(RM) $(DEPENDENCIES)

$(LIBRARY): $(OBJECTS)
	$(AR) rs $(LIBRARY) $(OBJECTS)

include $(DEPTH)/make_in/Makefile.targets
