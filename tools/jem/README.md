# JEMalloc Developer Tools

This is a set of command-line tools for analyzing memory usage over time
of programs incorporating the
[JEMalloc](http://www.canonware.com/jemalloc/ "JEMalloc Website")
memory allocator. The tools work by analyzing a sequence of snapshots of
the state of the allocator as reported by the `malloc_stats_print()` API.

These tools are written in the Python 2.X language.

The main JEMalloc GitHub repo. is:
[https://github.com/jemalloc/jemalloc](https://github.com/jemalloc/jemalloc)

While these tools can be used with any program that uses JEMalloc and
logs the appropriate statistics to a file, the specific examples in this
documentation will be in the context of the
[Aerospike server](https://github.com/aerospike/aerospike-server).

JEMalloc is incorporated as a submodule in the Aerospike server (version
3.0 or greater) as `modules/jemalloc`.

Aerospike's public fork of the main JEMalloc repo. is:
[https://github.com/aerospike/jemalloc](https://github.com/aerospike/jemalloc).

# Usage

The typical way to use these tools is first to collect from a program
some sequence of samples of JEMalloc statistics over time, and then to
interactively examine the collected data using this set of tools. (Each
time sample of the state of JEMalloc memory usage is termed an "iteration"
by these tools.)

The Aerospike server's current JEMalloc statistics can be dumped to the
console log file (`/tmp/aerospike-console.<PID>`) using the Info. command
`jem-stats:`, e.g., by using the command `asinfo -v jem-stats:`.

Running the `get-jem-stats` script will periodically trigger collection
of these statistics to a file by the Aerospike server using the
`jem-stats:` Info. command.

Next, running the script `extract-jem-stats` will pre-process the log
file containing the JEMalloc statistics into a Python file suitable for
inclusion by the this set of tools.

Finally, the time series of JEMalloc statistics samples may be analyzed
using the tools provided by this tool set.

The three tools are: `jemabs` (<em>abs</em>olute memory usage), `jemdel`
(<em>del</em>ta memory usage), and `jemeff` (memory usage <em>eff</em>iciency.)

## Tool Command-Line Arguments

The common command-line argument pattern for each of these tools is:

`{{<JEMDataFile:Default:'jemdata'>} <ThreholdMB:Default=200> <StartIteration:Default=0> <EndIteration:Default=<Last>>}`

* All arguments are optional.

* If the first argument is non-numeric, it will be interpreted as the
base source filename (without ".py") in the current directory containing
the pre-processed JEMalloc statistics.

* The first numeric argument is interpreted as a minimum threshold (in
megabytes) for a value to be significant enough to output. (The
threshold is compared against the absolute value of a data point, so
negative values in the data can also be observed.)

* The second numeric argument is the starting iteration, which defaults to 0.

* The third numeric argument is the ending iteration, which defaults to
the last iteration of the data set.

The output of the tools in a "human readable" format, i.e., in terms of
"MB", "GB", etc.

These tools currently look at the `total`, `active`, and `mapped` memory
as reported by JEMalloc, on an arena-by-arena basis. These quantities
give a good overall picture of what is happening in an arena over time.

The same techniques could in principle be extended to cover other
statistics reported by JEMalloc.

# Examples

	prompt% extract-jem-stats      --  Pre-process the log file for use by these tools.

	prompt% jemdel                 --  Print all changes in arena sizes greater than 200MB.

	prompt% jemabs 1024 3 6        --  Print the arena sizes greater than 1GB for iterations 3 through 5, inclusive.

# Files

The files in this directory are as follows:

* `get-jem-stats` -- Poll the Aerospike server (version 3.0 or greater)
to dump its JEMalloc statistics to its console log file on a time
interval (defaulting to 600 sec. = 10 min.)  (TCSH version.)

* `get-jem-stats.sh` -- (BASH version of the above.)

* `extract-jem-stats` -- Pre-process the JEMalloc statistics into the
file `jemdata.py` suitable for processing by the `jem(abs|del|eff)`
tools. (TCSH version.)

* `extract-jem-stats.sh` -- (BASH version of the above.)

* `jemabs` -- Calculate the absolute greatest sizes of memory allocated
for each JEMalloc arena.

* `jemdel` -- Calculate the greatest changes (`delta`) in memory
allocated for each JEMalloc arena.

* `jemeff` -- Calculate the efficiency (memory used / memory allocated)
for each JEMalloc arena.

* `jemdefs.py` -- Definitions needed by the `jem(abs|del|eff)` tools.

* `README.md` -- This documentation file.
