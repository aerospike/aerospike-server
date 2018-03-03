#!/bin/bash
#
#    File:   extract-jem-stats.sh
#
#    Description:
#       Extract JEMalloc statistics from the file specified as the single
#       command-line argument, which defaults to the latest Aerospike server
#       console log file.
#
#       Output is to the file 'jemdata.py' in the current working directory,
#       suitable for processing by the 'jem(abs|del|eff)' tools."
#
#    Usage:
#       prompt$ extract-jem-stats.sh {<JEMallocStatsFile:Default=/tmp/aerospike-console.<PID>>}
#

shopt -s expand_aliases
alias ascons='\ls -t /tmp/aerospike-console.* | head -1'

if [ $# -eq 0 ]; then
   FILE=`ascons`
elif [ $# -eq 1 ]; then
  FILE=$1
  if [ ! -f $FILE ]; then
     echo "Error!  File $FILE not found!"
     exit -1
  fi
else
  echo "Usage:  $0 {<JEMallocStatsFile>}"
  echo "  Extract JEMalloc statistics from the given file, which defaults to"
  echo "  the latest Aerospike server console log file.  Output is to the file"
  echo "  'jemdata.py' in the current working directory, suitable for processing"
  echo "  by the 'jem(abs|del|eff)' tools."
  exit -1
fi

echo -n "Extracting data from $FILE...."

egrep '(^(arenas|(total|active|mapped):)|Begin)' $FILE | sed "s/.*Begin.*/arenas 'sum'/" | tr -d ':' | sed 's/\[\([0-9]*\)\]/ \1/' | awk 'BEGIN{print "def load(arenas, total, active, mapped):"}{print "    "$1"("$2") "}' > jemdata.py

echo "Done."
