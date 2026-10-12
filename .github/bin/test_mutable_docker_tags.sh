#!/usr/bin/env bash
#
# Tests for mutable_docker_tags.sh, against a local fixture repo cloned so that its
# branches appear under refs/remotes/origin/ the way a full actions/checkout leaves them.
#
# The ownership cases matter most: two lines that both write a tag flip it every night,
# and an unattended run that writes w.x.y.z can repoint a released image. The fixture's
# hotfix branches have their own history and tags, and each case runs with its own ref
# checked out, so ranking against the wrong commit gives a different answer.

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/mutable_docker_tags.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
pass() { echo "✓ $1"; }
fail() {
    echo "✗ $1"
    failures=$((failures + 1))
}

git_quiet() { git -c init.defaultBranch=master -c user.name=t -c user.email=t@t "$@" >/dev/null 2>&1; }

UP="${WORK}/upstream"
CLONE="${WORK}/clone"
mkdir -p "$UP"
git_quiet init "$UP"

# tagged_commit <tag>: an empty commit on the current branch carrying a bare w.x.y.z tag.
tagged_commit() {
    git_quiet -C "$UP" commit --allow-empty -m "$1"
    git_quiet -C "$UP" tag -a "$1" -m "$1"
}

# fork <branch> <tag>: branch from master's current commit and give it its own tagged
# commit, so describe on the branch and on master disagree.
fork() {
    git_quiet -C "$UP" checkout -b "$1"
    tagged_commit "$2"
    git_quiet -C "$UP" commit --allow-empty -m "after $2"
    git_quiet -C "$UP" checkout master
}

tagged_commit 8.0.0.0
fork hotfix/8.0.0 8.0.0.21
tagged_commit 8.1.1.0
fork hotfix/8.1.1 8.1.1.9
tagged_commit 8.1.2.0
fork hotfix/8.1.2 8.1.2.6
git_quiet -C "$UP" branch hotfix/8.1.2.escrow hotfix/8.1.2
tagged_commit 8.2.0.0
fork hotfix/8.2.0 8.2.0.1
git_quiet -C "$UP" branch hotfix/8.2.0.escrow hotfix/8.2.0
tagged_commit 8.2.1.0
git_quiet -C "$UP" commit --allow-empty -m "after the bare tag"
git_quiet clone "$UP" "$CLONE"

# run <ref> <env...>: the script with <ref> checked out and a clean environment, so a
# variable exported by the caller can't leak into a case.
run() {
    local ref=$1
    shift
    git_quiet -C "$CLONE" checkout --detach "origin/${ref}"
    (cd "$CLONE" && env -u UNATTENDED -u MODE -u REF_NAME -u PACKAGE_VERSION \
        REF_NAME="$ref" "$@" bash "$SCRIPT")
}

# expect <name> <want> <ref> <env...>: want is the exact stdout, "" for "move nothing".
expect() {
    local name=$1 want=$2 got rc
    shift 2
    got="$(run "$@" 2>"${WORK}/err")"
    rc=$?
    if [[ "$rc" -eq 0 && "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: rc=${rc} got '${got}' want '${want}' -- $(cat "${WORK}/err")"
    fi
}

# expect_error <name> <stderr pattern> <ref> <env...>
expect_error() {
    local name=$1 pattern=$2
    shift 2
    if run "$@" >/dev/null 2>"${WORK}/err"; then
        fail "${name}: accepted"
    elif grep -q -- "$pattern" "${WORK}/err"; then
        pass "$name"
    else
        fail "${name}: failed without naming the reason: $(cat "${WORK}/err")"
    fi
}

expect "master takes w.x.y.z, w.x.y, w.x and latest" "8.2.1.0 8.2.1 8.2 latest" \
    master MODE=auto PACKAGE_VERSION=8.2.1.0-88
expect "an unattended master run does not move w.x.y.z" "8.2.1 8.2 latest" \
    master MODE=auto PACKAGE_VERSION=8.2.1.0-88 UNATTENDED=true
expect "master moves nothing for a hotfix line's version" "" \
    master MODE=auto PACKAGE_VERSION=8.1.2.6-1
expect "master moves nothing for a version with no line" "" \
    master MODE=auto PACKAGE_VERSION=8.1.3.0-1
expect "a hotfix behind master's line does not take w.x" "8.2.0.1 8.2.0" \
    hotfix/8.2.0 MODE=auto PACKAGE_VERSION=8.2.0.1-3
expect "the newest hotfix line of its family takes w.x, past an .escrow sibling" "8.1.2.6 8.1.2 8.1" \
    hotfix/8.1.2 MODE=auto PACKAGE_VERSION=8.1.2.6-1
expect "an older hotfix line of the family does not take w.x" "8.1.1.9 8.1.1" \
    hotfix/8.1.1 MODE=auto PACKAGE_VERSION=8.1.1.9-1
expect "a hotfix line alone in its family takes w.x" "8.0.0.21 8.0.0 8.0" \
    hotfix/8.0.0 MODE=auto PACKAGE_VERSION=8.0.0.21-1
expect "an unattended hotfix run does not move w.x.y.z" "8.1.2 8.1" \
    hotfix/8.1.2 MODE=auto PACKAGE_VERSION=8.1.2.6-1 UNATTENDED=true
expect "a hotfix ref that is not hotfix/<w.x.y> moves nothing" "" \
    hotfix/8.2.0.escrow MODE=auto PACKAGE_VERSION=8.2.0.1-1
expect "force does not widen a hotfix line's set" "8.2.0.1 8.2.0" \
    hotfix/8.2.0 MODE=force PACKAGE_VERSION=8.2.0.1-3
expect "a missing MODE behaves as auto" "8.2.0.1 8.2.0" \
    hotfix/8.2.0 PACKAGE_VERSION=8.2.0.1-3
expect "auto moves nothing on a preview ref" "" \
    master MODE=auto PACKAGE_VERSION=8.2.1.0-88-g0f294c425-1 REF_NAME=preview/x
expect "force on a preview ref takes the master set" "8.2.1.0 8.2.1 8.2 latest" \
    master MODE=force PACKAGE_VERSION=8.2.1.0-88-g0f294c425-1 REF_NAME=preview/x
if grep -q "8.2.1.0 only with that line's next manual run, never a nightly" "${WORK}/err"; then
    pass "force on a preview ref warns that only a manual run reclaims w.x.y.z"
else
    fail "force on a preview ref did not warn: $(cat "${WORK}/err")"
fi

expect_error "a descriptor on master fails closed" "not a w.x.y.z-<n> release coordinate" \
    master MODE=auto PACKAGE_VERSION=8.2.1.0-88-g0f294c425-1
expect_error "a legacy -start anchor on a hotfix line fails closed under auto" "legacy -start" \
    hotfix/8.1.1 MODE=auto PACKAGE_VERSION=8.1.1.3-start-5-g24ece0a08-1
expect_error "a legacy -start anchor on a hotfix line fails closed under force" "legacy -start" \
    hotfix/8.1.1 MODE=force PACKAGE_VERSION=8.1.1.3-start-5-g24ece0a08-1
expect_error "an unparseable version fails closed" "cannot derive w.x.y.z" \
    master MODE=auto PACKAGE_VERSION=garbage

# Overlap: master is still on 8.2.1 when hotfix/8.2.1 is cut. The hotfix owns the line.
git_quiet -C "$UP" branch hotfix/8.2.1
git_quiet -C "$CLONE" fetch origin
expect "master yields its line to hotfix/<its w.x.y> and keeps latest" "latest" \
    master MODE=auto PACKAGE_VERSION=8.2.1.0-90
expect "force on master does not override the overlap" "latest" \
    master MODE=force PACKAGE_VERSION=8.2.1.0-90
expect "an unattended master run during the overlap moves only latest" "latest" \
    master MODE=auto PACKAGE_VERSION=8.2.1.0-90 UNATTENDED=true
expect "hotfix/<master's w.x.y> takes the whole line, w.x included" "8.2.1.1 8.2.1 8.2" \
    hotfix/8.2.1 MODE=auto PACKAGE_VERSION=8.2.1.1-2

# Without any w.x.y.z tag there is no line to rank against.
git -C "$CLONE" tag -l | while read -r t; do git_quiet -C "$CLONE" tag -d "$t"; done
expect_error "a hotfix run without master's w.x.y.z tag fails closed" "cannot resolve master's w.x.y.z" \
    hotfix/8.1.2 MODE=auto PACKAGE_VERSION=8.1.2.6-1
expect_error "a master run without its w.x.y.z tag fails closed" "cannot resolve master's w.x.y.z" \
    master MODE=auto PACKAGE_VERSION=8.2.1.0-88

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "all tests passed"
