#!/usr/bin/env bash

total=0

if [ -f .build.yml ]; then
    # grep -c returns 0 if no matches, and won't break the script
    x86=$(grep -c 'aerospike-server:x86' .build.yml || true)
    arm=$(grep -c 'aerospike-server:arm' .build.yml || true)
    total=$((x86 * 3 + arm * 2))
fi
echo "$total"
