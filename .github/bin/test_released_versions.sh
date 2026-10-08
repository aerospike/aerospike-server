#!/usr/bin/env bash
#
# Tests for released_versions.sh.
#
# `curl`, `jf` and `git` are stubbed as BINARIES on PATH, so the real fetchers run --
# the pagination loop, the Link-header parse, jq, registry_tags.sh and all. An earlier
# version of this file replaced the three fetcher FUNCTIONS instead, which meant the code
# that actually talks to a registry was never executed and the page-2 failure below went
# undetected.
#
# The property under test is one the callers bet a public registry on: this either returns
# the COMPLETE union or fails. A partial answer is the dangerous one -- a release missing
# from the union makes the tip come out low, and a caller then moves w.x / latest backwards
# onto a superseded build, with every digest check still passing.

# Every case runs the script in its own subshell with its env injected, so those
# assignments are deliberately subshell-local (SC2030/SC2031).
# shellcheck disable=SC2030,SC2031
set -uo pipefail

BIN="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${BIN}/released_versions.sh"
WORK="$(mktemp -d)"
STUB="${WORK}/bin"
PAGES="${WORK}/pages"
mkdir -p "$STUB"
trap 'rm -rf "$WORK"' EXIT

failures=0
pass() { echo "✓ $1"; }
fail() {
    echo "✗ $1"
    failures=$((failures + 1))
}

# Docker Hub: a token request, then one request per page. PAGES is a directory of
# <cursor>.body files, with an optional <cursor>.rc to make that request fail and an
# optional <cursor>.next naming the cursor that follows.
cat >"${STUB}/curl" <<'STUB'
#!/usr/bin/env bash
hdrs="" url=""
while [[ $# -gt 0 ]]; do
    case "$1" in
    -D) hdrs="$2"; shift 2 ;;
    -H) shift 2 ;;
    -*) shift ;;
    *) url="$1"; shift ;;
    esac
done
if [[ "$url" == *auth.docker.io* ]]; then
    # Spelled out rather than via ${TOKEN_JSON:-...}: a `}` inside a parameter
    # expansion's default terminates the expansion early and emits malformed JSON.
    if [[ -n "${TOKEN_JSON:-}" ]]; then
        printf '%s' "$TOKEN_JSON"
    else
        printf '{"token":"t"}'
    fi
    exit 0
fi
n=1
if [[ "$url" == *last=* ]]; then
    n="${url##*last=}"
    n="${n%%&*}"
fi
[[ ! -f "${PAGES}/${n}.rc" ]] || exit "$(cat "${PAGES}/${n}.rc")"
# Real curl truncates the -D file on every request; so must this, or a stale Link
# header makes the caller follow the same cursor forever.
[[ -z "$hdrs" ]] || : >"$hdrs"
if [[ -f "${PAGES}/${n}.next" && -n "$hdrs" ]]; then
    printf 'link: </v2/testorg/img/tags/list?last=%s&n=1000>; rel="next"\n' \
        "$(cat "${PAGES}/${n}.next")" >"$hdrs"
fi
cat "${PAGES}/${n}.body"
STUB

# Artifactory, reached through registry_tags.sh.
cat >"${STUB}/jf" <<'STUB'
#!/usr/bin/env bash
[[ -z "${JF_STDERR:-}" ]] || printf '%s\n' "$JF_STDERR" >&2
[[ "${JF_RC:-0}" -eq 0 ]] || exit "${JF_RC}"
printf '%s' "${JF_JSON:-[]}"
STUB

cat >"${STUB}/git" <<'STUB'
#!/usr/bin/env bash
[[ "${GIT_RC:-0}" -eq 0 ]] || exit "${GIT_RC}"
printf '%s\n' ${GIT_TAGS:-}
STUB
chmod +x "${STUB}/curl" "${STUB}/jf" "${STUB}/git"

page() { # page <cursor> <body> [next-cursor]
    mkdir -p "$PAGES"
    printf '%s' "$2" >"${PAGES}/${1}.body"
    [[ -z "${3:-}" ]] || printf '%s' "$3" >"${PAGES}/${1}.next"
}
page_fails() {
    mkdir -p "$PAGES"
    printf '%s' "${2:-22}" >"${PAGES}/${1}.rc"
    : >"${PAGES}/${1}.body"
}

run_rv() {
    (
        set +u
        export PATH="${STUB}:${PATH}" PAGES="$PAGES"
        export RV_HUB_ORG=testorg RV_PROD_REPO=test-prod-repo RV_IMAGES="img-a"
        export RV_GIT_DIR=.
        export GIT_TAGS="$GIT_TAGS" GIT_RC="${GIT_RC:-0}"
        export JF_JSON="$JF_JSON" JF_RC="${JF_RC:-0}" JF_STDERR="${JF_STDERR:-}"
        export TOKEN_JSON="${TOKEN_JSON:-}"
        bash "$SCRIPT"
    ) 2>"${WORK}/err"
}

rv_case() {
    rm -rf "$PAGES"
    unset GIT_RC JF_RC JF_STDERR TOKEN_JSON
    GIT_TAGS="v8.1.2.5"
    JF_JSON='[{"path":"test-prod-repo/img-a/8.1.2.5/list.manifest.json"}]'
    page 1 '{"tags":["8.1.2.5"]}'
}

expect_ok() {
    local name=$1 want=$2 got rc
    got="$(run_rv)"
    rc=$?
    if [[ "$rc" -ne 0 ]]; then
        fail "${name}: exited ${rc} -- $(tail -2 "${WORK}/err")"
    elif [[ "$got" != "$want" ]]; then
        fail "${name}: got [${got//$'\n'/,}], want [${want//$'\n'/,}]"
    else
        pass "$name"
    fi
}

expect_fail() {
    local name=$1 want=$2 rc err
    run_rv >/dev/null
    rc=$?
    err="$(cat "${WORK}/err")"
    if [[ "$rc" -eq 0 ]]; then
        fail "${name}: succeeded, expected a non-zero exit"
    elif [[ "$err" != *"$want"* ]]; then
        fail "${name}: stderr did not mention '${want}': ${err}"
    else
        pass "$name"
    fi
}

# A release recorded in only ONE of the three places still counts: that is the whole
# reason the union exists.
rv_case
GIT_TAGS="v8.1.2.1 v8.1.2.2"
JF_JSON='[{"path":"test-prod-repo/img-a/8.1.2.1/list.manifest.json"}]'
page 1 '{"tags":["8.1.2.1","8.1.2.3"]}'
expect_ok "union across all three sources" $'8.1.2.1\n8.1.2.2\n8.1.2.3'

# The June/July 2026 window: Docker Hub had 8.1.2.3 while the ship tag said 8.1.2.2 was
# newest. A git-only read calls 8.1.2.2 the tip and drags 8.1/latest onto it.
rv_case
GIT_TAGS="v8.1.2.0 v8.1.2.1 v8.1.2.2"
JF_JSON='[{"path":"test-prod-repo/img-a/8.1.2.2/list.manifest.json"}]'
page 1 '{"tags":["8.1.2.3"]}'
expect_ok "a registry-only release beats a lagging ship tag" $'8.1.2.0\n8.1.2.1\n8.1.2.2\n8.1.2.3'

# Every page is read. The listing is not version-ordered, so a release can sit behind a
# thousand build tags on the last page.
rv_case
page 1 '{"tags":["8.1.2.5","8.1.2.5-20260915181406"]}' c2
page c2 '{"tags":["8.2.0.0"]}'
expect_ok "pagination reaches the last page" $'8.1.2.5\n8.2.0.0'

# THE failure this file exists for. Page 1 succeeds, page 2 does not. Returning page 1
# looks like success and silently loses 8.2.0.0 from the union.
rv_case
page 1 '{"tags":["8.1.2.5"]}' c2
page_fails c2
expect_fail "a failure on page 2 aborts rather than returning page 1" "could not list Docker Hub tags"

# A last page legitimately carries "tags": null; a page that is not JSON must not be
# mistaken for it.
rv_case
page 1 '{"tags":["8.1.2.5","8.2.0.0"]}' c2
page c2 '{"tags":null}'
expect_ok "a null last page is a valid end of listing" $'8.1.2.5\n8.2.0.0'

rv_case
page 1 '<html>502 Bad Gateway</html>'
expect_fail "a non-JSON page aborts" "unparseable JSON"

rv_case
TOKEN_JSON='{}'
expect_fail "no pull token aborts" "no pull token"

rv_case
JF_RC=1 JF_STDERR="401 Unauthorized"
expect_fail "a failed jf listing aborts" "could not list"

rv_case
JF_JSON='not json'
expect_fail "an unparseable jf listing aborts" "not the JSON array"

# jf writes warnings to stderr. Folded into stdout they would corrupt the JSON jq parses.
rv_case
JF_STDERR="[Warn] something routine"
expect_ok "a jf warning on stderr does not corrupt the listing" "8.1.2.5"

rv_case
GIT_RC=1
expect_fail "a failed git listing aborts" "could not be listed"

# Each source must contribute something: all three carry every supported lineage, so an
# empty answer means the listing was read wrong, not that there are no releases.
rv_case
GIT_TAGS=""
expect_fail "empty git ship tags abort" "git ship tags"
rv_case
JF_JSON='[]'
expect_fail "an empty PROD listing aborts" "test-prod-repo/img-a"
rv_case
page 1 '{"tags":[]}'
expect_fail "an empty Docker Hub listing aborts" "docker.io/testorg/img-a"

# A listing of only non-release tags is indistinguishable from an empty one.
rv_case
JF_JSON='[{"path":"test-prod-repo/img-a/latest/list.manifest.json"},{"path":"test-prod-repo/img-a/8.1/list.manifest.json"}]'
expect_fail "PROD with no release-shaped tag aborts" "test-prod-repo/img-a"

# Build artifacts, not release coordinates. Docker Hub carries both shapes today, and
# both version-sort ABOVE the release they were cut from. sha256__ folders are what
# Artifactory stores per-platform manifests under.
rv_case
JF_JSON='[{"path":"test-prod-repo/img-a/8.1.2.5/list.manifest.json"},{"path":"test-prod-repo/img-a/sha256__abc/manifest.json"}]'
page 1 '{"tags":["8.1.2.5","latest","8.1","8.1.2.6-rc1","8.1.2.5.1","8.2.0.0_20260922T013041Z"]}'
expect_ok "non-release tag shapes are filtered out" "8.1.2.5"

# sort -V, not sort: lexically 8.1.2.10 < 8.1.2.9.
rv_case
GIT_TAGS="v8.1.2.9 v8.1.2.10 v8.1.2.2"
JF_JSON='[{"path":"test-prod-repo/img-a/8.1.2.9/list.manifest.json"}]'
page 1 '{"tags":["8.1.2.9"]}'
expect_ok "version-sorted, not lexically" $'8.1.2.2\n8.1.2.9\n8.1.2.10'

# Required env is a hard precondition: without RV_PROD_REPO the listing would be of
# "/img-a" and come back empty, failing for the wrong stated reason.
for missing in RV_HUB_ORG RV_PROD_REPO; do
    rv_case
    err=$(
        set +u
        export PATH="${STUB}:${PATH}" PAGES="$PAGES"
        export RV_HUB_ORG=testorg RV_PROD_REPO=test-prod-repo RV_GIT_DIR=.
        export GIT_TAGS="$GIT_TAGS" JF_JSON="$JF_JSON"
        unset "$missing"
        bash "$SCRIPT" 2>&1 >/dev/null
    )
    if [[ "$err" == *"$missing"* ]]; then
        pass "unset ${missing} aborts and names it"
    else
        fail "unset ${missing}: stderr did not name it: ${err}"
    fi
done

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "All released_versions tests passed."
