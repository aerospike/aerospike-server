/*
 * stream.h
 *
 * Copyright (C) 2012-2014 Aerospike, Inc.
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
/*
 * This file implements stream parsing for rows
 */

#pragma once

#include "ai_obj.h"
#include "bt.h"

int u160Cmp (void *s1, void *s2);
int llCmp   (void *s1, void *s2);
int ylCmp   (void *s1, void *s2);

char *createBTKey(ai_obj *key, bool *med, uint32 *ksize, bt *btr, btk_t *btk);
void  destroyBTKey(char *btkey, bool  med);

void   convertStream2Key(uchar *stream, ai_obj *key, bt *btr);
uchar *parseStream(uchar *stream, bt *btr);
void  *createStream(bt *btr, void *val, char *btkey, uint32 klen, uint32 *ssize, crs_t *crs);
bool   destroyStream(bt *btr, uchar *ostream);
