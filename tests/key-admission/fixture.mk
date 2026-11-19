# Run from as/src so the server's ordinary build settings apply.
include Makefile

TEST_DIR = $(DEPTH)/tests/key-admission
FIXTURE_OBJECT = $(OBJECT_DIR)/admission_fixture.o
FIXTURE = $(BIN_DIR)/asd-admission-fixture
WRAPPED = as_read_start as_write_start as_partition_reserve_read_tr \
	as_partition_reserve_write as_partition_release as_transaction_error \
	rw_request_hash_insert

$(FIXTURE_OBJECT): $(TEST_DIR)/fixture.c
	$(CC) $(CFLAGS) $(DEF_FN) -o $@ -c $(INCLUDES) $<

-include $(FIXTURE_OBJECT:.o=.d)

$(FIXTURE): $(OBJECTS) $(AS_LIB_DEPS) $(FIXTURE_OBJECT) $(TEST_DIR)/fixture.mk
	$(LINK.c) -o $@ $(OBJECTS) $(FIXTURE_OBJECT) $(LIBRARIES) $(WRAPPED:%=-Wl,--wrap=%)

.PHONY: test-admission
test-admission: $(SERVER) $(FIXTURE)
	python3 $(TEST_DIR)/run.py $(SERVER) $(FIXTURE) $(SCHEMA_FILE)
