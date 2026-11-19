#include <assert.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include "base/transaction.h"
#include "base/datamodel.h"
#include "citrusleaf/alloc.h"
#include "fabric/partition.h"
#include "transaction/rw_request_hash.h"

extern transaction_status __real_as_read_start(as_transaction* tr);
extern transaction_status __real_as_write_start(as_transaction* tr);
extern int __real_as_partition_reserve_read_tr(as_namespace*, uint32_t,
		as_transaction*, cf_node*);
extern int __real_as_partition_reserve_write(as_namespace*, uint32_t,
		as_partition_reservation*, cf_node*);
extern void __real_as_partition_release(as_partition_reservation*);
extern void __real_as_transaction_error(as_transaction*, as_namespace*, uint32_t);

static __thread as_partition_reservation* reserved;

int
__wrap_as_partition_reserve_read_tr(as_namespace* ns, uint32_t pid,
		as_transaction* tr, cf_node* dest)
{
	int rv = __real_as_partition_reserve_read_tr(ns, pid, tr, dest);
	if (rv == 0) {
		reserved = &tr->rsv;
	}
	return rv;
}

int
__wrap_as_partition_reserve_write(as_namespace* ns, uint32_t pid,
		as_partition_reservation* rsv, cf_node* dest)
{
	int rv = __real_as_partition_reserve_write(ns, pid, rsv, dest);
	if (rv == 0) {
		reserved = rsv;
	}
	return rv;
}

void
__wrap_as_partition_release(as_partition_reservation* rsv)
{
	if (reserved == rsv) {
		reserved = NULL;
	}
	__real_as_partition_release(rsv);
}

void
__wrap_as_transaction_error(as_transaction* tr, as_namespace* ns, uint32_t code)
{
	if (code == AS_ERR_KEY_BUSY) {
		assert(reserved == NULL);
	}
	__real_as_transaction_error(tr, ns, code);
}

static const uint8_t hot_digest[] = {
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
};

static bool
test_key(const as_transaction* tr)
{
	return tr->origin == FROM_CLIENT && strcmp(tr->rsv.ns->name, "test") == 0 &&
			memcmp(tr->keyd.digest, hot_digest, sizeof(hot_digest)) == 0;
}

static void
mark(const char* marker)
{
	int fd = open(marker, O_CREAT | O_WRONLY | O_APPEND, 0600);
	assert(fd >= 0);
	ssize_t written = write(fd, ".", 1);
	assert(written == 1);
	close(fd);
}

static void
hold(const char* marker, const char* entered)
{
	if (access(marker, F_OK) == 0) {
		mark(entered);
		while (access(marker, F_OK) == 0) {
			usleep(1000);
		}
	}
}

extern transaction_status __real_rw_request_hash_insert(rw_request_hkey*,
		struct rw_request_s*, as_transaction*);

transaction_status
__wrap_rw_request_hash_insert(rw_request_hkey* key, struct rw_request_s* rw,
		as_transaction* tr)
{
	bool watch = test_key(tr) && access("hold-insertion", F_OK) == 0;
	transaction_status status = __real_rw_request_hash_insert(key, rw, tr);

	if (watch && status == TRANS_IN_PROGRESS) {
		hold("hold-insertion", "inserted");
	}
	else if (watch && status == TRANS_WAITING) {
		mark("queued");
	}

	return status;
}

transaction_status
__wrap_as_read_start(as_transaction* tr)
{
	if (test_key(tr)) {
		hold("hold-read", "entered-read");
	}

	return __real_as_read_start(tr);
}

transaction_status
__wrap_as_write_start(as_transaction* tr)
{
	if (test_key(tr)) {
		hold("hold-write", "entered-write");
		if ((tr->from_flags & FROM_FLAG_RESTART) != 0) {
			hold("hold-restart", "restarted");
		}

		if (access("async-write", F_OK) == 0) {
			// Simulate an async owner completing before the handler returns.
			as_msg_send_reply(tr->from.proto_fd_h, AS_OK, 0, 0, NULL, NULL,
					0, NULL, NULL);
			__wrap_as_partition_release(&tr->rsv);
			cf_free(tr->msgp);
			return TRANS_IN_PROGRESS;
		}
	}

	return __real_as_write_start(tr);
}
