# Aerospike Server - Aerospike Index
# Makefile

DEPTH = ../..
include $(DEPTH)/make_in/Makefile.in

HEADERS = ai_btree.h ai_types.h ai_obj.h bt.h bt_iterator.h bt_output.h btree.h btreepriv.h stream.h

SOURCES = ai_btree.c ai_obj.c bt.c bt_code.c bt_iterator.c bt_output.c stream.c

INCLUDES += $(INCLUDE_DIR:%=-I%)
INCLUDES += -I$(CF)/include -I$(AS)/include
INCLUDES += -I$(AS)/include
INCLUDES += -I$(COMMON)/target/$(PLATFORM)/include
INCLUDES += -I$(MOD_LUA)/target/$(PLATFORM)/include

LIBRARY = $(LIBRARY_DIR)/libai.a

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
