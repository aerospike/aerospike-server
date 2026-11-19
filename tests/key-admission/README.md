# Single-record key admission tests

From the repository root on Linux, initialize the submodules and install the [server build prerequisites](../../README.md). The tests also require Python 3.8 or newer, GNU ld and a non-loopback network interface for automatic node ID selection.

```sh
git submodule update --init --recursive
make -j4
make -C as/src -f ../../tests/key-admission/fixture.mk test-admission
```

The suite starts temporary Community Edition servers on loopback ports. It exercises legacy and YAML startup with the ordinary server binary and reads back both limits. A second binary links the same server objects with test-only wrappers that pause individual handlers. This makes overlap, saturation, live changes, wait-queue restart and error cleanup observable through client requests, counters and logs. The fixture simulates an immediate asynchronous write completion. XDR and MRT cases exercise admission with client message flags set. Batch coverage uses a real batch read.
