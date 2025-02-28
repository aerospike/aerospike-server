# Aerospike Database Server

Welcome to the Aerospike Database Server source code tree!

Aerospike is a distributed, scalable NoSQL database. It is architected with three key objectives:

- To create a high-performance, scalable platform that would meet the needs of today's web-scale applications
- To provide the robustness and reliability (i.e., ACID) expected from traditional databases.
- To provide operational efficiency (minimal manual involvement)

For more information on Aerospike, please visit: [`http://aerospike.com`](http://aerospike.com)

## Build Prerequisites

The Aerospike Database Server can be built and deployed on various
current 64-bit GNU/Linux platform versions, such as Red Hat Enterprise Linux 8/9, Amazon Linux 2023, 
Debian 11 or later, and Ubuntu 20.04 or later.

### Dependencies

The majority of the Aerospike source code is written in the C
programming language, conforming to the ANSI C99 standard.

To install dependencies for a development environment run
`./bin/install-dependencies.sh` in the aerospike-server repo.

In particular, the following tools and libraries are needed:

#### C Compiler Toolchain

Building Aerospike requires the GCC 4.1 or later C compiler toolchain,
with the standard GNU/Linux development tools and libraries installed in
the build environment, including:

* `autoconf`

* `automake`

* `cmake`

* `libtool`

* `make`

#### C++

The C++ compiler is required for the Aerospike geospatial indexing
feature and its dependency, Google's S2 Geometry Library (both written in C++.)

* The Red Hat Enterprise Linux 8/9 requires `gcc-c++`.

* The Debian 11/12 and Ubuntu 20/22/24 requires `g++`.

#### OpenSSL

OpenSSL 0.9.8b or later is required for cryptographic hash functions
(RIPEMD-160 & SHA-1) and pseudo-random number generation.

* The Red Hat Enterprise Linux 8/9 requires `openssl-devel`

* The Debian 11/12 and Ubuntu 20/22/24 requires `libssl-dev`.

#### Zlib

* The Red Hat Enterprise Linux 8/9 requires `zlib-devel`

* The Debian 11/12 and Ubuntu 20/22/24 requiresi `zlib1g-dev`.

### Submodules

The Aerospike Database Server build depends upon the following submodules:

| Submodule | Description |
|---------- | ----------- |
| abseil-cpp | Support for the S2 Spherical Geometry Library |
| common    | The Aerospike Common Library |
| jansson   | C library for encoding, decoding and manipulating JSON data |
| jemalloc  | The JEMalloc Memory Allocator |
| libbacktrace | A C library that may be linked into a C/C++ program to produce symbolic backtraces |
| lua       | The Lua runtime |
| mod-lua   | The Aerospike Lua Interface |
| s2geometry | The S2 Spherical Geometry Library |

After the initial cloning of the `aerospike-server` repo., the
submodules must be fetched for the first time using the following
command:

	$ git submodule update --init

*Note:*  As this project uses submodules, the source archive downloadable
via GitHub's `Download ZIP` button will not build unless the correct
revision of each submodule is first manually installed in the appropriate
`modules` subdirectory.

## Building Aerospike

### Default Build

	$ make          -- Perform the default build (no packaging.)

*Note:* You can use the `-j` option with `make` to speed up the build
on multiple CPU cores. For example, to run four parallel jobs:

    $ make -j4

### Build Options

	$ make deb      -- Build the Debian (Ubuntu) package.

	$ make rpm      -- Build the Red Hat Package Manager (RPM) package.

	$ make source   -- Package the source code as a compressed "tar" archive.

	$ make clean    -- Delete any existing build products, excluding built packages.

	$ make cleanpkg -- Delete built packages.

	$ make cleanall -- Delete all existing build products, including built packages.

	$ make cleangit -- Delete all files untracked by Git.  (Use with caution!)

	$ make strip    -- Build a "strip(1)"ed version of the server executable.

### Overriding Default Build Options

	$ make {<Target>}* {<VARIABLE>=<VALUE>}*  -- Build <Target>(s) with optional variable overrides.

#### Example:

	$ make  -- Default build.

## Configuring Aerospike

Sample Aerospike configuration files are provided in `as/etc`.  The
developer configuration file, `aerospike_dev.conf`, contains basic
settings that should work out-of-the-box on most systems. The package
example configuration files, `aerospike.conf`, and the Solid State Drive
(SSD) version, `aerospike_ssd.conf`, are suitable for running Aerospike
as a system daemon.

These sample files may be modified for specific use cases (e.g., setting
network addresses, defining namespaces, and setting storage engine
properties) and tuned for for maximum performance on a particular
system.  Also, system resource limits may need to be increased to allow,
e.g., a greater number of concurrent connections to the database.  See
"man limits.conf" for how to change the system's limit on a process'
number of open file descriptors ("nofile".)

## Running Aerospike

There are several options for running the Aerospike database. Which
option to use depends upon whether the primary purpose is production
deployment or software development.

The preferred method for running Aerospike in a production environment
is to build and install the Aerospike package appropriate for the target
Linux distribution (i.e., an `".rpm"`, `".deb"`, or `".tgz"` file), and
then to control the state of the Aerospike daemon via `systemctl` on
e.g., `systemctl start aerospike`.

Please refer to the full documentation on the Aerospike web site,
[`https://docs.aerospike.com/`](https://docs.aerospike.com/), for more
detailed information about configuring and running the Aerospike
Database Server, as well as about the Aerospike client API packages
for popular programming languages.
