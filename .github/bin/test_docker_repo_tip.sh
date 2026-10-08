#!/usr/bin/env bash
#
# Tests for docker_repo_tip.sh, against local fixture repos -- git ls-remote works on a
# filesystem path, so nothing here touches the network.
#
# The failure cases matter more than the success one: this resolves the ref that three
# separate jobs then execute code from, so an empty or malformed answer must stop the run
# rather than become a checkout of something unintended.

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/docker_repo_tip.sh"
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
mkdir -p "$UP"
git_quiet init "$UP"
echo one >"${UP}/f"
git_quiet -C "$UP" add f
git_quiet -C "$UP" commit -m one
echo two >"${UP}/f"
git_quiet -C "$UP" add f
git_quiet -C "$UP" commit -m two
WANT="$(git -C "$UP" rev-parse master)"

got="$(DOCKER_REPO_URL="$UP" bash "$SCRIPT" 2>"${WORK}/err")"
rc=$?
if [[ "$rc" -eq 0 && "$got" == "$WANT" ]]; then
    pass "resolves master's tip to its sha"
else
    fail "resolves master's tip: rc=${rc} got '${got}' want '${WANT}' -- $(cat "${WORK}/err")"
fi

if [[ "$got" =~ ^[0-9a-f]{40}$ ]]; then
    pass "the answer is a full 40-hex sha, which is what the callers validate"
else
    fail "the answer is not 40-hex: '${got}'"
fi

# A second commit must change the answer -- otherwise this is not tracking anything.
echo three >"${UP}/f"
git_quiet -C "$UP" add f
git_quiet -C "$UP" commit -m three
moved="$(DOCKER_REPO_URL="$UP" bash "$SCRIPT" 2>/dev/null)"
if [[ "$moved" != "$got" && "$moved" == "$(git -C "$UP" rev-parse master)" ]]; then
    pass "the answer follows the branch"
else
    fail "the answer did not follow the branch: '${moved}'"
fi

# ls-remote exits 0 with no output when nothing matches, so "no such branch" would come
# back as an empty ref and be checked out as the default branch.
if DOCKER_REPO_URL="$UP" DOCKER_REPO_BRANCH=nope bash "$SCRIPT" >/dev/null 2>"${WORK}/err"; then
    fail "a branch that does not exist was accepted"
elif grep -q "has no nope branch" "${WORK}/err"; then
    pass "a branch that does not exist fails closed"
else
    fail "a missing branch failed without naming the reason: $(cat "${WORK}/err")"
fi

if DOCKER_REPO_URL="${WORK}/not-a-repo" bash "$SCRIPT" >/dev/null 2>"${WORK}/err"; then
    fail "an unreachable repository was accepted"
elif grep -q "could not reach" "${WORK}/err"; then
    pass "an unreachable repository fails closed"
else
    fail "an unreachable repository failed without naming the reason: $(cat "${WORK}/err")"
fi

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "All docker_repo_tip tests passed."
