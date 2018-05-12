# Aerospike Database Server

Welcome to the Aerospike Database Server source code tree!

Aerospike is a distributed, scalable NoSQL database. It is architected with three key objectives:

- To create a high-performance, scalable platform that would meet the needs of today's web-scale applications
- To provide the robustness and reliability (i.e., ACID) expected from traditional databases.
- To provide operational efficiency (minimal manual involvement)

For more information on Aerospike, please visit: [`http://aerospike.com`](http://aerospike.com)

## Telemetry Anonymized Data Collection

The Aerospike Community Edition collects anonymized server performance statistics.
Please see the
[Aerospike Telemetery web page](http://aerospike.com/aerospike-telemetry) for more
information.  The full Telemetry data collection agent source code may be found in the
["telemetry" submodule](https://github.com/aerospike/aerospike-telemetry-agent/blob/master/README.md).

## Build Prerequisites

The Aerospike Database Server can be built and deployed on various
current 64-bit GNU/Linux platform versions, such as the Red Hat family (e.g.,
CentOS 6 or later), Debian 7 or later, and Ubuntu 14.04 or later.

### Dependencies

The majority of the Aerospike source code is written in the C
programming language, conforming to the ANSI C99 standard.

In particular, the following tools and libraries are needed:

#### C Compiler Toolchain

Building Aerospike requires the GCC 4.1 or later C compiler toolchain,
with the standard GNU/Linux development tools and libraries installed in
the build environment, including:

* `autoconf`

* `automake`

* `libtool`

* `make`

#### C++

The C++ compiler is required for the Aerospike geospatial indexing
feature and its dependency, Google's S2 Geometry Library (both written in C++.)

* The required CentOS 6/7 package to install is: `gcc-c++`.

* The required Debian 7/8/9 and Ubuntu 14/16/18 package to install is: `g++`.

#### OpenSSL

OpenSSL 0.9.8b or later is required for cryptographic hash functions
(RIPEMD-160 & SHA-1) and pseudo-random number generation.

* The CentOS 6/7 OpenSSL packages to install are:  `openssl`,
`openssl-devel`, `openssl-static`.

* The Debian 7/8/9 and Ubuntu 14/16/18 OpenSSL packages to install are:
`openssl` and `libssl-dev`.

#### Lua 5.1

The [Lua](http://www.lua.org) 5.1 language is required for User Defined
Function (UDF) support.

* By default, Aerospike builds with Lua 5.1 support provided by the
[LuaJIT](http://luajit.org) submodule.

* Alternatively, it is possible to build with standard Lua 5.1 provided
by the build environment.  In that case:

	* The CentOS 6/7 Lua packages to install are:  `lua`,
`lua-devel`, and `lua-static`.

	* The Debian 7/8/9 and Ubuntu 14/16/18 Lua packages to install are:
`lua5.1` and `liblua5.1-dev`.

	* Build by passing the `USE_LUAJIT=0` option to `make`.

#### zlib

Building on Ubuntu 18 also requires installing `libz-dev`.

#### Python 2

Running the Telemetry Agent requires Python 2.6+, which is available by default on most
platforms, and can be installed on Ubuntu 16+ as the package `python`.

### Submodules

The Aerospike Database Server build depends upon 8 submodules:

| Submodule | Description |
|---------- | ----------- |
| common    | The Aerospike Common Library |
| jansson   | C library for encoding, decoding and manipulating JSON data |
| jemalloc  | The JEMalloc Memory Allocator |
| lua-core  | The Aerospike Core Lua Source Files |
| luajit    | The LuaJIT (Just-In-Time Compiler for Lua) |
| mod-lua   | The Aerospike Lua Interface |
| s2-geometry-library | The S2 Spherical Geometry Library |
| telemetry | The Aerospike Telemetry Agent (Community Edition only) |

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

	$ make tar      -- Build the "Every Linux" compressed "tar" archive (".tgz") package.

	$ make source   -- Package the source code as a compressed "tar" archive.

	$ make clean    -- Delete any existing build products, excluding built packages.

	$ make cleanpkg -- Delete built packages.

	$ make cleanall -- Delete all existing build products, including built packages.

	$ make cleangit -- Delete all files untracked by Git.  (Use with caution!)

	$ make strip    -- Build "strip(1)"ed versions of the server executables.

### Overriding Default Build Options

	$ make {<Target>}* {<VARIABLE>=<VALUE>}*  -- Build <Target>(s) with optional variable overrides.

#### Example:

	$ make USE_JEM=0   -- Default build *without* JEMalloc support.

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
then to control the state of the Aerospike daemon, either via the SysV
daemon init script commands, e.g., `service aerospike start`, or else
via `systemctl` on `systemd`-based systems, e.g., `systemctl start aerospike`.

A convenient way to run Aerospike in a development environment is to use
the following commands from within the top-level directory of the source
code tree (`aerospike-server`):

To create and initialize the `run` directory with the files needed for
running Aerospike, use:

	$ make init

or, equivalently:

	$ mkdir -p run/{log,work/{smd,{sys,usr}/udf/lua}}
	$ cp -pr modules/lua-core/src/* run/work/sys/udf/lua

To launch the server with `as/etc/aerospike_dev.conf` as the config:

	$ make start

or, equivalently:

	$ nohup ./modules/telemetry/telemetry.py as/etc/telemetry_dev.conf > /dev/null 2>&1 &
	$ target/Linux-x86_64/bin/asd --config-file as/etc/aerospike_dev.conf

To halt the server:

	$ make stop

or, equivalently:

	$ PID=`pgrep telemetry.py | grep -v grep`; if [ -n "$PID" ]; then kill $PID; fi
	$ kill `cat run/asd.pid` ; rm run/asd.pid

Please refer to the full documentation on the Aerospike web site,
[`http://aerospike.com/docs/`](http://aerospike.com/docs/), for more
detailed information about configuring and running the Aerospike
Database Server, as well as about the Aerospike client API packages
for popular programming languages.

