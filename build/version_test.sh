#!/usr/bin/env bash
#
# Regression checks for build/version and build/gen_version.
# Run from the repo root: bash build/version_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

failures=0

check() {
    local label="$1"
    shift
    if "$@"; then
        echo "OK   $label"
    else
        echo "FAIL $label"
        failures=$((failures + 1))
    fi
}

check_not_v_prefixed() {
    local label="$1"
    local out="$2"
    if [[ "$out" != v* ]]; then
        echo "OK   $label"
    else
        echo "FAIL $label"
        failures=$((failures + 1))
    fi
}

# build/version -v  → bare version (w.x.y.z on master/hotfix; describe on dev)
# build/version     → full version (w.x.y.z-n on master/hotfix; describe on dev)
# build/version -r  → build number only

bare="$(bash build/version -v)"
full="$(bash build/version)"
bnum="$(bash build/version -r)"

check_not_v_prefixed "version -v output is not v-prefixed" "$bare"
check_not_v_prefixed "version (default) output is not v-prefixed" "$full"
echo "     version -v  = $bare"
echo "     version     = $full"
echo "     version -r  = $bnum"

build_id="$(bash build/gen_version 2>/dev/null |
    grep 'aerospike_build_id' |
    sed -n 's/.*"\([^"]*\)".*/\1/p')"
check "gen_version build_id matches build/version (default)" test "$build_id" = "$full"
echo "     build_id=$build_id"

branch="$(git branch --show-current)"
if [[ -z "$branch" ]]; then
    branch="$(git branch -r --contains HEAD | grep -v HEAD | head -1 |
        sed -e 's/origin\///' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
fi

if [[ "$branch" == master || "$branch" =~ ^hotfix/ ]]; then
    check "master/hotfix: -v has no describe suffix" test "$bare" = "${bare%%-*}"
    check "master/hotfix: -b is a positive integer" [[ "$bnum" =~ ^[0-9]+$ ]]
    check "master/hotfix: default equals -v plus -b" test "$full" = "${bare}-${bnum}"
else
    check "dev branch: -v includes describe suffix" \
        grep -qE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+-g[0-9a-f]+$' <<<"$bare"
    check "dev branch: default matches -v" test "$full" = "$bare"
    check "dev branch: -r is 1" test "$bnum" = "1"
fi

if [[ $failures -eq 0 ]]; then
    echo "All checks passed."
    exit 0
fi

echo "$failures check(s) failed."
exit 1
