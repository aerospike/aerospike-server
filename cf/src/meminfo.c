/*
 * meminfo.c
 *
 * Copyright (C) 2008 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

#include "meminfo.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


int
cf_meminfo(uint64_t *physmem, uint64_t *freemem, int *freepct, bool *swapping)
{
	// do this without a malloc, because we might be in trouble, malloc-wise
	char buf[4096];
	memset(buf, 0, sizeof(buf)); // makes valgrind happy?

	// be a little oversafe
	if (physmem) *physmem = 0;
	if (freemem) *freemem = 0;
	if (freepct) *freepct = 0;
	if (swapping) *swapping = 0;

	// open /proc/meminfo
	int fd = open("/proc/meminfo", O_RDONLY , 0 /*mask not used if not creating*/ );
	if (fd < 0) {
		fprintf(stderr, "meminfo failed: can't open proc file\n");
		return(-1);
	}

	// this loop is overkill. proc read won't block, realistically
	int pos = 0, lim = sizeof(buf);
	int rv = 0;
	do {

		rv = read(fd, &buf[pos], lim - pos);
		if (rv > 0)
			pos += rv;
		else if (rv < 0) {
			fprintf(stderr, "meminfo failed: read returned %d errno %d pos %d\n",rv,errno,pos);
			close(fd);
			return(-1);
		}

	} while ((rv > 0) && (pos < lim));

	close(fd);

	char *physMemStr = "MemTotal"; uint64_t physMem = 0;
	char *freeMemStr = "MemFree"; uint64_t freeMem = 0;
	char *activeMemStr = "Active"; uint64_t activeMem = 0;
	char *inactiveMemStr = "Inactive"; uint64_t inactiveMem = 0;
	char *cachedMemStr = "Cached"; uint64_t cachedMem = 0;
	char *buffersMemStr = "Buffers"; uint64_t buffersMem = 0;
	char *swapTotalStr = "SwapTotal"; uint64_t swapTotal = 0;
	char *swapFreeStr = "SwapFree"; uint64_t swapFree = 0;
	char *sharedMemStr = "Shmem"; uint64_t sharedMem = 0;

	// parse each line - always three tokens, the name, the integer, and 'kb'
	char *cur = buf;
	char *saveptr = 0, *tok1, *tok2, *tok3;
	do {
		tok1 = tok2 = tok3 = 0;
		tok1 = strtok_r(cur,": \r\n" , &saveptr);
		cur = 0;
		tok2 = strtok_r(cur,": \r\n" , &saveptr);
		tok3 = strtok_r(cur,": \r\n" , &saveptr);

		if (tok1 && tok3) {
			if (strcmp(tok1, physMemStr) == 0)
				physMem = atoi(tok2);
			else if (strcmp(tok1, freeMemStr) == 0)
				freeMem = atoi(tok2);
			else if (strcmp(tok1, swapTotalStr) == 0)
				swapTotal = atoi(tok2);
			else if (strcmp(tok1, swapFreeStr) == 0)
				swapFree = atoi(tok2);
			else if (strcmp(tok1, activeMemStr) == 0)
				activeMem = atoi(tok2);
			else if (strcmp(tok1, inactiveMemStr) == 0)
				inactiveMem = atoi(tok2);
			else if (strcmp(tok1, cachedMemStr) == 0)
				cachedMem = atoi(tok2);
			else if (strcmp(tok1, buffersMemStr) == 0)
				buffersMem = atoi(tok2);
			else if (strcmp(tok1, sharedMemStr) == 0)
				sharedMem = atoi(tok2);
		}

	} while(tok1 && tok2 && tok3);

	//
	// Calculate available memory:
	//   Start with the total physical memory in the system.
	//   Next, subtract out the total of the active and inactive VM.
	//   Finally, add back in the cached memory and buffers, which are effectively available if & when needed.
	//   Caution: Subtract the shared memory, which is included in the cached memory, but is not available.
	//
	uint64_t availableMem = physMem - activeMem - inactiveMem + cachedMem + buffersMem - sharedMem;

	if (physmem) *physmem = physMem * 1024L;
	if (freemem) *freemem = availableMem * 1024L;

	// just easier to do this kind of thing in one place
	if (freepct) *freepct = (100L * availableMem) / physMem;

	if (swapping) {
		*swapping = false;
#if 0
		uint64_t swapUsedPct = ((swapTotal - swapFree)*100)/swapTotal;
		if (swapUsedPct > 10) {
			*swapping = true;
			fprintf(stderr, " SWAPPING: %"PRIu64" %"PRIu64" %"PRIu64,
				swapUsedPct, swapTotal, swapFree);
		}
#else
		// Silence compiler warnings.
		(void) swapFree;
		(void) swapTotal;
		(void) freeMem;
#endif
	}

//	fprintf(stderr, "%u swapTotal %u swapFree %u swapFreePct ::: swapping %d\n",
//		(unsigned int) swapTotal,(unsigned int)swapFree,(int)swapUsedPct,(int) *swapping);

	return(0);
}
