/* 
 * Copyright 2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef __throwstream_h
#define __throwstream_h		1

#include <iostream>
#include <sstream>

// The throwstream macro assembles the string argument to the
// exception constructor from an iostream.
//
#define throwstream(__except, __msg)				\
	do {											\
		std::ostringstream __ostrm;					\
		__ostrm << __msg;							\
		throw __except(__ostrm.str().c_str());		\
	} while (false)

#endif // __throwstream_h
