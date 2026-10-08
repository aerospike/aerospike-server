#!/usr/bin/env bash
#
# Tests for require_docker_ref_on_master.sh -- the only thing standing between a fork
# commit and code execution in jobs that hold a live JFrog OIDC token, SNYK_TOKEN and
# an Artifactory registry login.
#
# The fixtures reproduce the checkout shape the workflows actually use: actions/checkout
# with fetch-depth: 1, i.e. `git init` + `fetch --depth=1 origin <sha>` + checkout
# FETCH_HEAD. The shallow history is deliberate -- a check that only worked on a full
# clone would pass here and fail in CI.
#
# Case "sibling branch" is the one that fails if someone swaps the merge-base operands,
# appends `|| true`, or drops `set -e`. That is the whole point of this file.

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/require_docker_ref_on_master.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
pass() { echo "✓ $1"; }
fail() {
    echo "✗ $1"
    failures=$((failures + 1))
}

git_quiet() { git -c init.defaultBranch=master -c user.name=t -c user.email=t@t "$@" >/dev/null 2>&1; }

commit_in() {
    local repo=$1 msg=$2
    echo "$msg" >"${repo}/file"
    git_quiet -C "$repo" add file
    git_quiet -C "$repo" commit -m "$msg"
}

# An upstream with five commits on master and one commit on a branch forked off
# master~2 -- a fork commit reachable through the repository network, which is exactly
# what the script's header says actions/checkout will happily fetch.
build_upstream() {
    local up="${WORK}/upstream"
    rm -rf "$up"
    mkdir -p "$up"
    git_quiet init "$up"
    # --filter=tree:0 needs the serving side to allow partial clones; GitHub does.
    git_quiet -C "$up" config uploadpack.allowfilter true
    git_quiet -C "$up" config uploadpack.allowanysha1inwant true
    local i
    for i in 1 2 3 4 5; do commit_in "$up" "master ${i}"; done
    git_quiet -C "$up" checkout -b sidebranch master~2
    commit_in "$up" "fork commit"
    git_quiet -C "$up" checkout master
    echo "$up"
}

# What actions/checkout@v7 does with fetch-depth: 1 and a ref.
shallow_checkout() {
    local up=$1 ref=$2 dir="${WORK}/checkout"
    rm -rf "$dir"
    mkdir -p "$dir"
    git_quiet init "$dir"
    git_quiet -C "$dir" remote add origin "$up"
    git_quiet -C "$dir" fetch --depth=1 origin "$ref" || return 1
    git_quiet -C "$dir" checkout --detach FETCH_HEAD || return 1
    echo "$dir"
}

run_check() {
    local dir=$1 ref=$2
    DOCKER_REPO_DIR="$dir" DOCKER_REPO_REF="$ref" bash "$SCRIPT" 2>&1
}

UP="$(build_upstream)"

expect() {
    local name=$1 want_rc=$2 ref=$3 want_err=${4:-}
    local dir out rc
    dir="$(shallow_checkout "$UP" "$ref")" || {
        fail "${name}: fixture checkout of ${ref} failed"
        return
    }
    out="$(run_check "$dir" "$ref")"
    rc=$?
    if [[ "$want_rc" -eq 0 && "$rc" -ne 0 ]]; then
        fail "${name}: rc=${rc}, expected 0 -- ${out}"
    elif [[ "$want_rc" -ne 0 && "$rc" -eq 0 ]]; then
        fail "${name}: rc=0, expected non-zero (the ref was accepted)"
    elif [[ -n "$want_err" && "$out" != *"$want_err"* ]]; then
        fail "${name}: output did not mention '${want_err}': ${out}"
    else
        pass "$name"
    fi
}

expect "master tip is accepted" 0 "$(git -C "$UP" rev-parse master)"
expect "a commit three back on master is accepted" 0 "$(git -C "$UP" rev-parse master~3)"
expect "a fork commit on a sibling branch is refused" 1 \
    "$(git -C "$UP" rev-parse sidebranch)" "docker_repo_ref"

# An upstream whose master is gone: the fetch itself must fail the script rather than
# be skipped. Without set -e / the fetch's own exit status, merge-base would compare
# against a stale or absent FETCH_HEAD and could pass.
NOMASTER="${WORK}/nomaster"
mkdir -p "$NOMASTER"
git_quiet init "$NOMASTER"
git_quiet -C "$NOMASTER" config uploadpack.allowfilter true
git_quiet -C "$NOMASTER" config uploadpack.allowanysha1inwant true
commit_in "$NOMASTER" "only commit"
git_quiet -C "$NOMASTER" branch -m master main
dir="$(shallow_checkout "$NOMASTER" main)" || fail "fixture: no-master checkout failed"
out="$(run_check "$dir" main)"
rc=$?
if [[ "$rc" -ne 0 ]]; then
    pass "an upstream with no master fails closed"
else
    fail "an upstream with no master: rc=0, expected non-zero -- ${out}"
fi

# The `: "${DOCKER_REPO_DIR:?}" "${DOCKER_REPO_REF:?}"` guards. Unset either and the
# script must refuse rather than operate on the current directory.
for missing in DOCKER_REPO_DIR DOCKER_REPO_REF; do
    out=$(
        export DOCKER_REPO_DIR="${WORK}/checkout" DOCKER_REPO_REF=deadbeef
        unset "$missing"
        bash "$SCRIPT" 2>&1
    )
    rc=$?
    if [[ "$rc" -ne 0 ]]; then
        pass "unset ${missing} fails closed"
    else
        fail "unset ${missing}: rc=0, expected non-zero -- ${out}"
    fi
done

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "All require_docker_ref_on_master tests passed."
