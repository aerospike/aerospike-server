/*
 * as_stap.h
 *
 * Copyright (C) 2015 Aerospike, Inc.
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

#pragma once

#if defined(USE_SYSTEMTAP)
#include <sys/sdt.h>
#include "probes.h"
#else
#define ASD_TRANS_DEMARSHAL(arg1,arg2,arg3)
#define ASD_QUERY_STARTING(arg1,arg2)
#define ASD_QUERY_QTRSETUP_STARTING(arg1,arg2)
#define ASD_QUERY_QTRSETUP_FINISHED(arg1,arg2)
#define ASD_QUERY_INIT(arg1,arg2)
#define ASD_QUERY_DONE(arg1,arg2,arg3)
#define ASD_QUERY_TRANS_DONE(arg1,arg2,arg3)
#define ASD_QUERY_QTR_ALLOC(arg1,arg2,arg3)
#define ASD_QUERY_QTR_FREE(arg1,arg2,arg3)
#define ASD_QUERY_IOREQ_STARTING(arg1,arg2)
#define ASD_QUERY_IOREQ_FINISHED(arg1,arg2)
#define ASD_QUERY_IO_STARTING(arg1,arg2)
#define ASD_QUERY_IO_NOTMATCH(arg1,arg2)
#define ASD_QUERY_IO_ERROR(arg1,arg2)
#define ASD_QUERY_IO_FINISHED(arg1,arg2)
#define ASD_QUERY_NETIO_STARTING(arg1,arg2)
#define ASD_QUERY_NETIO_FINISHED(arg1,arg2)
#define ASD_QUERY_ADDFIN(arg1,arg2)
#define ASD_QUERY_SENDPACKET_STARTING(arg1,arg2,arg3)
#define ASD_QUERY_SENDPACKET_CONTINUE(arg1,arg2)
#define ASD_QUERY_SENDPACKET_FINISHED(arg1)
#define ASD_SINDEX_MSGRANGE_STARTING(arg1,arg2)
#define ASD_SINDEX_MSGRANGE_FINISHED(arg1,arg2)
#endif
