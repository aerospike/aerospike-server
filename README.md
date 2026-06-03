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

Building Aerospike requires the GCC 5.1 or later C compiler toolchain,
with the standard GNU/Linux development tools and libraries installed in
the build environment, including:

- `autoconf`
- `automake`
- `cmake`
- `libtool`
- `make`

#### C++

The C++ compiler is required for the Aerospike geospatial indexing
feature and its dependency, Google's S2 Geometry Library (both written in C++.)
C++ is also used for yaml/json configuration parsing.

- The Red Hat Enterprise Linux 8/9 requires `gcc-c++`.
- The Debian 11/12/13 and Ubuntu 20/22/24 requires `g++`.

#### OpenSSL

OpenSSL 0.9.8b or later.

- The Red Hat Enterprise Linux 8/9 requires `openssl-devel`
- The Debian 11/12/13 and Ubuntu 20/22/24 requires `libssl-dev`.

#### Zlib

- The Red Hat Enterprise Linux 8/9 requires `zlib-devel`
- The Debian 11/12/13 and Ubuntu 20/22/24 requires `zlib1g-dev`.

### Submodules

The Aerospike Database Server build depends upon the following submodules:

| Submodule | Description |
|---------- | ----------- |
| abseil-cpp | Support for the S2 Spherical Geometry Library |
| common    | The Aerospike Common Library |
| jansson   | C library for encoding, decoding and manipulating JSON data |
| jemalloc  | The JEMalloc Memory Allocator |
| json      | nlohmann/json library for json in c++, used primarily for config parsing |
| jsonschema | pboettch/json-schema-validator used to validate configuration against our schemas |
| libbacktrace | A C library that may be linked into a C/C++ program to produce symbolic backtraces |
| lua       | The Lua runtime |
| mod-lua   | The Aerospike Lua Interface |
| s2geometry | The S2 Spherical Geometry Library |
| yamlcpp   | jbeder/yaml-cpp used to parse yaml config files |

After the initial cloning of the `aerospike-server` repo., the
submodules must be fetched for the first time using the following
command:

	$ git submodule update --init

> [!IMPORTANT]
> As this project uses submodules, the source archive downloadable
> via GitHub's `Download ZIP` button will not build unless the correct
> revision of each submodule is first manually installed in the appropriate
> `modules` subdirectory.

## Building Aerospike

> [!TIP]
> Aerospike collects telemetry information about builds. To opt out, the
> environment variable AEROSPIKE_TELEMETRY must be set to FALSE.

### Default Build

	$ make          -- Perform the default build (no packaging.)

> [!TIP]
> You can use the `-j` option with `make` to speed up the build
> on multiple CPU cores. For example, to run four parallel jobs:

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

### Configuration Formats

Aerospike supports two configuration formats:

- **Traditional .conf format**: Sample configuration files are provided in `as/etc`. The
developer configuration file, `aerospike_dev.conf`, contains basic
settings that should work out-of-the-box on most systems. The package
example configuration files, `aerospike.conf`, and the Solid State Drive
(SSD) version, `aerospike_ssd.conf`, are suitable for running Aerospike
as a system daemon.

- **YAML format**: Aerospike supports YAML-based configuration with JSON schema validation
as an experimental feature. This format provides structured configuration with automatic
validation against a schema file.

  To use YAML configuration:

  1. Enable the feature with the `--experimental` flag when starting the server
  2. Optionally specify a custom schema file with `--schema-file <file>` (default location:
     `/opt/aerospike/schema/aerospike_config_schema.json`)
  3. Supply a YAML based configuration file that adheres to the schema.

  **Converting existing configurations**: Use the `asconfig` tool to convert traditional
  `.conf` files to YAML format. The tool generates YAML files with metadata headers that
  include version information.

  **Schema validation**: The JSON schema file validates your YAML configuration at startup,
  catching configuration errors early and providing better error messages for invalid settings.

### Convert and update YAML for the server

The `asconfig` tool currently outputs an older yaml config format, which is not the same as the
experimental YAML format the server validates. To move from a traditional `.conf` file
to the server’s experimental YAML config:

1. Convert your `.conf` file with `asconfig`.
2. Update the generated YAML to match the server’s schema. For the authoritative format,
   reference `/opt/aerospike/schema/aerospike_config_schema.json`.

#### Required updates after running `asconfig`

1. **Convert arrays to maps (keyed objects)**:
   - `namespaces`, `namespaces[].sets`
   - `network.tls`
   - `xdr.dcs`, `xdr.dcs[].namespaces`
2. **Remove `name` fields** that were used inside array entries; the name now becomes the map key.
3. **Update logging sinks** to typed entries with `contexts` (see schema for valid types/contexts):
   - `type: console` uses only `contexts`
   - `type: file` requires `path` plus `contexts`
   - `type: syslog` supports `facility`, `path`, `tag`, and `contexts`
4. **(Optional) Use unit-aware values** for size/time settings. Optionally use `{ value, unit }`
   objects where supported. Plain numeric values are still valid. Refer to the schema
   for the authoritative list of unit-capable fields and allowed units.

#### Example: array → map conversion

```yaml
# asconfig output (not valid for the server’s experimental YAML)
namespaces:
  - name: test
    replication-factor: 2
```

```yaml
# server experimental YAML format
namespaces:
  test:
    replication-factor: 2
```

**Notes**:
- Do not include the `name` property inside the map value.

#### Example: minimal config conversion

YAML configuration as output by asconfig.

```yaml
# asconfig output (not valid for the server’s experimental YAML)
service:
  cluster-name: my-cluster
  pidfile: /var/run/aerospike/asd.pid

logging:
  - name: console
    any: info

network:
  service:
    addresses:
      - any
    port: 3000
  fabric:
    port: 3001
  heartbeat:
    mode: mesh
    port: 3002
    addresses:
      - local
    interval: 150
    timeout: 10

namespaces:
  - name: test
    replication-factor: 2
    storage-engine:
      type: memory
      data-size: 4294967296
```

The configuration after aligning it with the database schema.

```yaml
# server experimental YAML format
service:
  cluster-name: my-cluster
  pidfile: /var/run/aerospike/asd.pid

logging:
  - type: console
    contexts:
      any: info

network:
  service:
    addresses:
      - any
    port: 3000
  fabric:
    port: 3001
  heartbeat:
    mode: mesh
    port: 3002
    addresses:
      - local
    interval: 150
    timeout: 10

namespaces:
  test:
    replication-factor: 2
    storage-engine:
      type: memory
      data-size: 4294967296
```

#### Example: logging sinks

Logging section as output by asconfig.

```yaml
# asconfig output (not valid for the server’s experimental YAML)
logging:
  - name: console
    any: info
  - name: /var/log/aerospike/aerospike.log
    any: info
```

Logging section after aligning it with the database schema.

```yaml
# server experimental YAML format
logging:
  - type: console
    contexts:
      any: info
  - type: file
    path: /var/log/aerospike/aerospike.log
    contexts:
      any: info
  - type: syslog
    facility: local1
    path: /dev/log
    tag: asd
    contexts:
      any: info
```

See `/opt/aerospike/schema/aerospike_config_schema.json` for the full list of
logging contexts and the allowed syslog facilities.

#### Example: unit-aware values

The database's experimental yaml configuration format allows certain
configs to use values with units.

```yaml
# server experimental YAML unit-enabled fields
namespaces:
  test:
    storage-engine:
      data-size:
        value: 4
        unit: g

xdr:
  dcs:
    dc1:
      namespaces:
        test:
          ship-versions-interval:
            value: 1
            unit: h
```

See `/opt/aerospike/schema/aerospike_config_schema.json` for the definitive list of
fields that accept units, the allowed unit values, and the `{ value, unit }` form.

These sample files may be modified for specific use cases (e.g., setting
network addresses, defining namespaces, and setting storage engine
properties) and tuned for maximum performance on a particular system.
Also, system resource limits may need to be increased to allow,
e.g., a greater number of concurrent connections to the database. See
"man limits.conf" for how to change the system's limit on a process'
number of open file descriptors ("nofile".)

## Running Aerospike

There are several options for running the Aerospike database. Which
option to use depends upon whether the primary purpose is production
deployment or software development.

The preferred method for running Aerospike in a production environment
is to build and install the Aerospike package appropriate for the target
Linux distribution (i.e., an `".rpm"` or `".deb"` file), and then to
control the state of the Aerospike daemon via `systemctl` on
e.g., `systemctl start aerospike`.

Please refer to the full documentation on the Aerospike web site,
[`https://docs.aerospike.com/`](https://docs.aerospike.com/), for more
detailed information about configuring and running the Aerospike
Database Server, as well as about the Aerospike client API packages
for popular programming languages.
