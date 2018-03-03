#!/bin/bash
#
#    File:   get-jem-stats.sh
#
#    Description:
#       Endlessly poll the Aerospike server for its JEMalloc statistics.
#       Polling interval is specified by the single, optional command-line
#       argument, which defaults to 600 sec. = 10 min.
#
#    Usage:
#       prompt$ get-jem-stats.sh {<PollIntervalSec:Default=600>}
#

INTERVAL=600
if [ $# -eq 1 ]; then INTERVAL=$1; fi

while true; do
  asinfo -v jem-stats: &> /dev/null
  sleep $INTERVAL
done
