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
check "gen_version build_id matches build/version -v (bare)" test "$build_id" = "$bare"
echo "     build_id=$build_id"

branch="$(git branch --show-current)"
if [[ -z "$branch" ]]; then
    branch="$(git branch -r --contains HEAD | grep -v HEAD | head -1 |
        sed -e 's/origin\///' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
fi

if [[ "$branch" == master || "$branch" =~ ^hotfix/ ]]; then
    check "master/hotfix: -v has no describe suffix" test "$bare" = "${bare%%-*}"
    # `[[` is a bash keyword, not a command, so it cannot be invoked through the
    # check helper's `"$@"`. Use a real command (grep) with the same semantics.
    check "master/hotfix: -r is a positive integer" grep -qE '^[0-9]+$' <<<"$bnum"
    check "master/hotfix: default equals -v plus -r" test "$full" = "${bare}-${bnum}"
else
    check "dev branch: -v includes describe suffix" \
        grep -qE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+-g[0-9a-f]+$' <<<"$bare"
    check "dev branch: default matches -v" test "$full" = "$bare"
    check "dev branch: -r is 1" test "$bnum" = "1"
fi

# Forced release-line path.
#
# The ambient-branch checks above only exercise the master/hotfix invariant when
# CI happens to run on such a ref. On a pull_request the checkout is a detached
# merge ref, so `git branch --show-current` is empty, the remote-branch fallback
# finds nothing, and the dev-branch path runs — where -v and default already
# match, making the core "compiled build_id stays bare while JFrog uses the full
# w.x.y.z-n" invariant untested. Force the release-line classification (via the
# AEROSPIKE_VERSION_FORCE_BRANCH test hook in build/version) so the invariant is
# checked from any checkout, using the real bare x.y.z.w tag reachable from HEAD
# (present in CI: fetch-depth 0 + fetch-tags).
export AEROSPIKE_VERSION_FORCE_BRANCH=master
rbare="$(bash build/version -v)"
rfull="$(bash build/version)"
rbnum="$(bash build/version -r)"
rbuild_id="$(bash build/gen_version 2>/dev/null |
    grep 'aerospike_build_id' |
    sed -n 's/.*"\([^"]*\)".*/\1/p')"
unset AEROSPIKE_VERSION_FORCE_BRANCH

check_not_v_prefixed "forced-release: -v output is not v-prefixed" "$rbare"
check "forced-release: -v has no describe suffix" test "$rbare" = "${rbare%%-*}"
check "forced-release: -r is a positive integer" grep -qE '^[0-9]+$' <<<"$rbnum"
check "forced-release: default equals -v plus -r" test "$rfull" = "${rbare}-${rbnum}"
# The regression this test exists to catch: gen_version must embed the bare -v
# version, not the full default. On the release-line path -v != default, so a
# revert of gen_version to default mode fails here (it passes silently on dev).
check "forced-release: gen_version build_id matches -v (bare)" test "$rbuild_id" = "$rbare"
echo "     forced version -v  = $rbare"
echo "     forced version     = $rfull"
echo "     forced version -r  = $rbnum"

if [[ $failures -eq 0 ]]; then
    echo "All checks passed."
    exit 0
fi

echo "$failures check(s) failed."
exit 1
