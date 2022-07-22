# How to build Aerospike with SPDK patch

The build process has been tested on Ubuntu 20.04

1. Install dependencies to build SPDK

```
git submodule update --init --recursive
sudo ./modules/spdk/scripts/pkgdep.sh
```

2. Apply Aerospike SPDK patch

```
patch -p1 < aerospike-spdk.patch
```

3. Build Aerospike with SPDK

```
make USE_SPDK=1 USE_LTHREAD=1
```

# How to setup Aerospike with SPDK

1. Create a SPDK config file for your NVMe device

```
sudo env HUGEMEM=20480 modules/spdk/scripts/setup.sh
sudo mkdir -p /usr/local/etc/spdk/
modules/spdk/scripts/gen_nvme.sh  --json-with-subsystems | sudo tee /usr/local/etc/spdk/aerospike.conf
```

2. Update a Aerospike config file

The developer configuration file, `aerospike_dev.conf`, contains basic settings.
You may want to change some parameters in the file.

For example, the `service-lcores` parameter specifies a list of CPU which the lightweight threads run on.
The `device` parameter specifies a storage device such as the SPDK block device (bdev).

# How to start Aerospike with SPDK

1. setup SPDK

This script only needs to be run once the system is up.

```
sudo env HUGEMEM=20480 modules/spdk/scripts/setup.sh
```

2. start Aerospike server

```
sudo su
make init
ulimit -n 200000
make start
```
