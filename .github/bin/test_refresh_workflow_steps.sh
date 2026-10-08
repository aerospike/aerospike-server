#!/usr/bin/env bash
#
# Tests for the decision logic embedded in the refresh workflows' `run:` blocks, lifted
# out with extract_run_block.py and executed against injected env and stubbed helpers.
#
# These steps decide, between them: whether the weekly cron publishes at all,
# whether an off-release-ref dispatch can push or promote, which versions get rebuilt,
# and which mutable tags -- including `latest` on docker.io -- move onto the result. Every one of those is a decision whose failure mode is a silently
# wrong but perfectly green run, and none of it is reachable from CI while it sits in
# YAML: a workflow_dispatch is a very expensive way to discover that a regex is wrong.

# Every step under test runs in its own subshell with its env injected, so those
# assignments are deliberately subshell-local (SC2030/SC2031).
# shellcheck disable=SC2030,SC2031
set -uo pipefail

BIN="$(cd "$(dirname "$0")" && pwd)"
WF_DISPATCHER="${BIN}/../workflows/refresh-docker-images.yaml"
WF_VERSION="${BIN}/../workflows/refresh-docker-image-version.yaml"
WF_PUBLISH="${BIN}/../workflows/publish-refreshed-tags.yaml"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
assertions=0
pass() {
    echo "✓ $1"
    assertions=$((assertions + 1))
}
fail() {
    echo "✗ $1"
    failures=$((failures + 1))
    assertions=$((assertions + 1))
}

block() {
    python3 "${BIN}/extract_run_block.py" "$1" "$2" || {
        echo "✗ could not extract step '$2' from $(basename "$1") -- renamed or removed?"
        exit 1
    }
}

# $GITHUB_OUTPUT is read back as `key=value`, the same way the workflow's consumers do.
out_of() { sed -n "s/^${2}=//p" "$1" | tail -1; }

###############################################################################
# refresh-docker-images.yaml :: Resolve effective settings
###############################################################################

SETTINGS="${WORK}/settings.sh"
block "$WF_DISPATCHER" "Resolve effective settings" >"$SETTINGS"

# Defaults match the workflow's own: a schedule event supplies no inputs at all, which
# for a boolean arrives as the empty string.
run_settings() {
    local out="${WORK}/settings.out"
    : >"$out"
    (
        set +u
        export GITHUB_OUTPUT="$out"
        export DEFAULT_DOCKER_REPO_REF="${DEFAULT_DOCKER_REPO_REF:-d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6}"
        export ENABLED="${ENABLED:-}" EVENT="${EVENT:-workflow_dispatch}" REF_NAME="${REF_NAME:-master}"
        export IN_VERSION="${IN_VERSION:-}" IN_ASADM_VERSION="${IN_ASADM_VERSION:-}"
        export IN_DOCKER_REPO_REF="${IN_DOCKER_REPO_REF:-}"
        export IN_PUSH_DOCKER_JFROG="${IN_PUSH_DOCKER_JFROG:-true}"
        export IN_PROMOTE_BUNDLE="${IN_PROMOTE_BUNDLE:-true}"
        export IN_FORCE_PUBLISH="${IN_FORCE_PUBLISH:-false}"
        bash "$SETTINGS"
    ) >"${WORK}/settings.log" 2>&1
}

settings_case() { unset ENABLED EVENT REF_NAME IN_VERSION IN_ASADM_VERSION IN_DOCKER_REPO_REF \
    IN_PUSH_DOCKER_JFROG IN_PROMOTE_BUNDLE IN_FORCE_PUBLISH; }

check_out() {
    local name=$1 key=$2 want=$3 got
    got="$(out_of "${WORK}/settings.out" "$key")"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: ${key}=${got@Q}, want ${want@Q}"
    fi
}

# The merge blocker: with the variable unset, the cron that lands with this file must do
# nothing at all.
settings_case
EVENT=schedule run_settings
check_out "schedule with REFRESH_DOCKER_IMAGES_ENABLED unset is disarmed" skip_all true

settings_case
EVENT=schedule ENABLED=false run_settings
check_out "schedule with the switch 'false' is disarmed" skip_all true

settings_case
EVENT=schedule ENABLED=TRUE run_settings
check_out "the switch is exact-match, not truthy" skip_all true

settings_case
ENABLED='' run_settings
check_out "a dispatch is never gated by the switch" skip_all false

# Armed, a scheduled run is a full publish against the pinned ref, whatever inputs say
# (a schedule event carries none).
settings_case
EVENT=schedule ENABLED=true IN_PUSH_DOCKER_JFROG=false IN_VERSION=8.1.2.5 run_settings
check_out "an armed schedule resolves the full-publish defaults" push_docker_jfrog true
check_out "an armed schedule ignores stray inputs (version)" version ""
check_out "an armed schedule promotes the bundle" promote_bundle true
check_out "an armed schedule takes the pinned ref" docker_repo_ref d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6
# force_publish is the one default a cron does not take: "identical to what is published"
# means there was nothing to ship, and republishing is churn on shared tags.
check_out "an armed schedule never forces a publish" force_publish false

# The release-ref policy: a dispatch from any branch must not reach PROD or public
# Docker Hub with that branch's own workflow code.
settings_case
REF_NAME=pvinh/SERVER-1520-refresh-docker-images run_settings
check_out "a branch dispatch cannot push to JFrog" push_docker_jfrog false
check_out "a branch dispatch cannot promote a bundle" promote_bundle false

settings_case
REF_NAME=master run_settings
check_out "master promotes" promote_bundle true

settings_case
REF_NAME=hotfix/8.1.2 run_settings
check_out "a hotfix line promotes" promote_bundle true

# preview/ is a release ref for build-sign-deploy because it publishes previews. A
# refresh only ever re-publishes an already-shipped release, so it is not one here.
settings_case
REF_NAME=preview/8.3 run_settings
check_out "preview/ is not a release ref for a refresh" promote_bundle false

# The pin lives in exactly one place now, so an empty input must land on it.
settings_case
IN_DOCKER_REPO_REF='' run_settings
check_out "an empty docker_repo_ref falls back to the pin" docker_repo_ref d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6

expect_settings_error() {
    local name=$1 want=$2
    if run_settings; then
        fail "${name}: accepted, expected a refusal"
    elif ! grep -qF "$want" "${WORK}/settings.log"; then
        fail "${name}: error did not mention '${want}': $(tail -2 "${WORK}/settings.log")"
    else
        pass "$name"
    fi
}

# A branch name resolves separately in `context` and `test-images`, hours apart, so it
# could name two different trees within one refresh.
settings_case
IN_DOCKER_REPO_REF=some/branch expect_settings_error "a branch name as docker_repo_ref is refused" "40-character commit sha or the literal 'master'"
settings_case
IN_DOCKER_REPO_REF=v8.1.2.5 expect_settings_error "a tag as docker_repo_ref is refused" "40-character commit sha or the literal 'master'"

# The one sentinel. It survives validation here and is turned into a sha by the resolve
# step, which is the only thing that may hand a ref to a checkout.
settings_case
IN_DOCKER_REPO_REF=master run_settings
check_out "the master sentinel is accepted" docker_repo_ref master
settings_case
IN_DOCKER_REPO_REF=d2a0368 expect_settings_error "an abbreviated sha is refused" "40-character commit sha"
settings_case
IN_DOCKER_REPO_REF=7E472D5AD886EACA4D944E8AC3FD994AF1F6A826 expect_settings_error "an uppercase sha is refused" "40-character commit sha"

# $GITHUB_OUTPUT is parsed line-wise with last-write-wins and has no newline guard, and
# a REST-dispatched string input may contain one. Validating before the write is what
# stops a forged `version=` line from flipping the two `version == ''` step guards.
settings_case
IN_VERSION=$'8.1.2.5\nversion=9.9.9.9' expect_settings_error "a newline in version is refused" "neither a w.x.y.z version nor a w.x lineage"
settings_case
IN_DOCKER_REPO_REF=$'d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6\nversion=8.1.2.5' expect_settings_error "a newline in docker_repo_ref is refused" "40-character commit sha"

settings_case
IN_VERSION=8.1.2 expect_settings_error "a three-segment version is refused" "neither a w.x.y.z version nor a w.x lineage"
settings_case
IN_ASADM_VERSION=latest expect_settings_error "a non-numeric asadm_version is refused" "not an asadm version"
settings_case
IN_PROMOTE_BUNDLE=yes expect_settings_error "a non-boolean toggle is refused by name" "promote_bundle resolved to 'yes'"

settings_case
IN_VERSION=8.1 run_settings
check_out "a w.x lineage is accepted" version 8.1
settings_case
IN_VERSION=8.1.2.5 run_settings
check_out "a w.x.y.z version is accepted" version 8.1.2.5

# Structural: the pin is a single literal. The whole point of dropping the input's
# default was that a partial bump silently had dispatches and the cron on different refs.
pin_count="$(grep -c 'd2a0368abedf9aa216f5cd7c7e97d86b0111cdd6' "$WF_DISPATCHER")"
if [[ "$pin_count" -eq 1 ]]; then
    pass "the docker_repo_ref pin appears exactly once in the dispatcher"
else
    fail "the docker_repo_ref pin appears ${pin_count} times; a partial bump would split dispatch from schedule"
fi

###############################################################################
# refresh-docker-images.yaml :: Resolve versions to refresh
###############################################################################

RES="${WORK}/resolve.sh"
block "$WF_DISPATCHER" "Resolve versions to refresh" >"$RES"

RVWS="${WORK}/rvws"
mkdir -p "${RVWS}/.github/bin"
cat >"${RVWS}/.github/bin/released_versions.sh" <<'STUB'
#!/usr/bin/env bash
[[ -z "${RELEASED:-}" ]] || printf '%s\n' ${RELEASED}
[[ -n "${RELEASED:-}" ]] || exit 1
STUB
chmod +x "${RVWS}/.github/bin/released_versions.sh"

run_resolve() {
    local out="${WORK}/resolve.out"
    : >"$out"
    (
        set +u
        cd "$RVWS" || exit 1
        export GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="${WORK}/resolve.summary"
        export VERSION="$VERSION" DOCKER_REPO_REF=deadbeef RELEASED="$RELEASED"
        bash "$RES"
    ) >"${WORK}/resolve.log" 2>&1
}

check_resolve() {
    local name=$1 want=$2 got
    got="$(out_of "${WORK}/resolve.out" versions)"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: versions=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/resolve.log")"
    fi
}

VERSION=8.1.2.5 RELEASED="8.1.2.5 8.2.0.0"
run_resolve
check_resolve "an explicit w.x.y.z is taken as given" '["8.1.2.5"]'

VERSION=8.1 RELEASED="8.1.2.4 8.1.2.5 8.2.0.0"
run_resolve
check_resolve "a lineage resolves to its newest release" '["8.1.2.5"]'

# The lineage anchor is a REGEX. Unescaped, `8.1` matches `8.10.0.0`, and a refresh of
# lineage 8.1 would pick a version from a different lineage entirely.
VERSION=8.1 RELEASED="8.1.2.5 8.10.0.0"
run_resolve
check_resolve "lineage 8.1 does not match 8.10.x" '["8.1.2.5"]'

VERSION=8.10 RELEASED="8.1.2.5 8.10.0.0"
run_resolve
check_resolve "lineage 8.10 resolves to its own release" '["8.10.0.0"]'

# sort -V within a lineage: 8.1.2.10 is newer than 8.1.2.9.
VERSION=8.1 RELEASED="8.1.2.9 8.1.2.10"
run_resolve
check_resolve "the lineage tip is version-sorted" '["8.1.2.10"]'

VERSION=8.9 RELEASED="8.1.2.5"
if run_resolve; then
    fail "a lineage with no release was accepted"
elif grep -q "no released w.x.y.z for lineage" "${WORK}/resolve.log"; then
    pass "a lineage with no release is refused"
else
    fail "a lineage with no release failed without naming the reason"
fi

VERSION=8.1.2 RELEASED="8.1.2.5"
if run_resolve; then
    fail "a three-segment version was accepted"
else
    pass "a three-segment version is refused"
fi

VERSION=8.1.2.5 RELEASED=""
if run_resolve; then
    fail "an unresolvable release set was accepted"
elif grep -q "could not resolve the released version set" "${WORK}/resolve.log"; then
    pass "an unresolvable release set aborts"
else
    fail "an unresolvable release set failed without naming the reason"
fi

###############################################################################
# refresh-docker-images.yaml :: the precheck filter
###############################################################################

FILTER="${WORK}/filter.sh"
block "$WF_DISPATCHER" "Select the versions whose published images are behind the archive" >"$FILTER"

# A fake workspace holding the two probes the step shells out to, plus a `docker` stub
# for the image cleanup.
WS="${WORK}/ws"
mkdir -p "${WS}/.github/bin" "${WS}/bin"
printf '#!/usr/bin/env bash\nexit 0\n' >"${WS}/bin/docker"
chmod +x "${WS}/bin/docker"

# EXISTS_MAP / PENDING_MAP: newline-separated "<ref-substring> <answer>" rules, first
# match wins, so a case can make one edition of one version behave differently.
cat >"${WS}/.github/bin/image_exists.sh" <<'STUB'
#!/usr/bin/env bash
while read -r pat ans; do
    [[ -n "$pat" ]] || continue
    [[ "$1" == *"$pat"* ]] || continue
    exit "$ans"
done <<<"${EXISTS_MAP:-}"
exit 0
STUB
cat >"${WS}/.github/bin/image_packages.sh" <<'STUB'
#!/usr/bin/env bash
# $1 = mode, $2 = ref
while read -r pat ans; do
    [[ -n "$pat" ]] || continue
    [[ "$2" == *"$pat"* ]] || continue
    [[ "$ans" != "error" ]] || exit 1
    for ((i = 0; i < ans; i++)); do echo "pkg${i} 1.0"; done
    exit 0
done <<<"${PENDING_MAP:-}"
exit 0
STUB
chmod +x "${WS}/.github/bin/image_exists.sh" "${WS}/.github/bin/image_packages.sh"

run_filter() {
    local out="${WORK}/filter.out"
    : >"$out"
    (
        set +u
        export GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="${WORK}/filter.summary"
        export GITHUB_WORKSPACE="$WS"
        export REGISTRY=artifact.example PROD_DOCKER_REPO=prod-repo
        export EVENT="${EVENT:-schedule}" VERSIONS="$VERSIONS"
        export EXISTS_MAP="${EXISTS_MAP:-}" PENDING_MAP="${PENDING_MAP:-}"
        export PATH="${WS}/bin:${PATH}"
        bash "$FILTER"
    ) >"${WORK}/filter.log" 2>&1
}

filter_case() {
    unset EVENT EXISTS_MAP PENDING_MAP
    VERSIONS='["8.1.2.5"]'
    : >"${WORK}/filter.summary"
}

check_filter() {
    local name=$1 want=$2 got
    got="$(out_of "${WORK}/filter.out" versions)"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: versions=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/filter.log")"
    fi
}

filter_case
EVENT=workflow_dispatch VERSIONS='["8.1.2.5","8.0.0.20"]' run_filter
check_filter "a dispatch is never second-guessed" '["8.1.2.5","8.0.0.20"]'

filter_case
VERSIONS='["8.1.2.5","8.0.0.20"]' PENDING_MAP="" run_filter
check_filter "every image current means nothing to refresh" "[]"

filter_case
VERSIONS='["8.1.2.5","8.0.0.20"]' PENDING_MAP="8.1.2.5 3" run_filter
check_filter "only the version that is behind is selected" '["8.1.2.5"]'

# One edition behind is enough: the three ship as a set.
filter_case
VERSIONS='["8.1.2.5"]' PENDING_MAP="aerospike-server-federal:8.1.2.5 2" run_filter
check_filter "one edition behind selects the version" '["8.1.2.5"]'

# A probe that cannot read an image must not be mistaken for "nothing to do" -- that
# parks a vulnerable image in the clear.
filter_case
VERSIONS='["8.1.2.5"]' PENDING_MAP="8.1.2.5 error" run_filter
check_filter "an unreadable image fails open" '["8.1.2.5"]'
if grep -q "could not inspect" "${WORK}/filter.log"; then
    pass "  and says so"
else
    fail "  without a warning naming it"
fi

# 1 from image_exists.sh is "could not tell" -- a 401, a 429, a 5xx. Also fail-open.
filter_case
VERSIONS='["8.1.2.5"]' EXISTS_MAP="8.1.2.5 1" run_filter
check_filter "an unreachable registry fails open" '["8.1.2.5"]'

# 2 is "the registry answered: not there". Fail-open would re-fire every Monday forever
# with nothing able to fix it, so it is named -- but as that LINEAGE's fault, not the
# run's. Failing here took every other lineage with it, because `dispatch` needs
# `precheck`: PROD has no 8.2.0.0 tag today while 8.2 is in RELEASES, so the first armed
# cron would have refreshed nothing at all.
filter_case
VERSIONS='["8.1.2.5","8.0.0.20"]' EXISTS_MAP="8.1.2.5 2" PENDING_MAP="8.0.0.20 3" run_filter
rc=$?
if [[ "$rc" -ne 0 ]]; then
    fail "an absent image failed the whole run: $(tail -3 "${WORK}/filter.log")"
else
    pass "an absent image does not fail the run"
fi
check_filter "  and the healthy lineage is still refreshed" '["8.0.0.20"]'
if [[ "$(out_of "${WORK}/filter.out" gaps)" == *8.1.2.5* ]]; then
    pass "  while the absent one is recorded as a gap"
else
    fail "  but the gap was not recorded: $(out_of "${WORK}/filter.out" gaps)"
fi
if grep -q "A refresh only re-publishes an existing release" "${WORK}/filter.log"; then
    pass "  and named"
else
    fail "  without naming it"
fi

filter_case
VERSIONS='["8.1.2.5"]' PENDING_MAP="8.1.2.5 2" run_filter
if [[ "$(out_of "${WORK}/filter.out" gaps)" == "[]" ]]; then
    pass "no gaps when every version is published"
else
    fail "gaps reported when there were none"
fi

# jq -r '.[]' <<<"" exits 0 with no output, so an empty list would walk into the
# "nothing to do" branch having examined nothing.
filter_case
VERSIONS='' run_filter
rc=$?
if [[ "$rc" -ne 0 ]] && grep -q "received no versions to examine" "${WORK}/filter.log"; then
    pass "an empty version list is an error, not 'nothing to do'"
else
    fail "an empty version list: rc=${rc} -- $(tail -3 "${WORK}/filter.log")"
fi

###############################################################################
# publish-refreshed-tags.yaml :: Decide which mutable tags may move
###############################################################################

NARROW="${WORK}/decide.sh"
block "$WF_PUBLISH" "Decide which mutable tags may move" >"$NARROW"

NWS="${WORK}/nws"
mkdir -p "${NWS}/.github/bin"
cat >"${NWS}/.github/bin/released_versions.sh" <<'STUB'
#!/usr/bin/env bash
[[ -z "${RELEASED:-}" ]] || printf '%s\n' ${RELEASED}
[[ -n "${RELEASED:-}" ]] || exit 1
STUB
chmod +x "${NWS}/.github/bin/released_versions.sh"

run_narrow() {
    local out="${WORK}/narrow.out"
    : >"$out"
    (
        set +u
        cd "$NWS" || exit 1
        export GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="${WORK}/narrow.summary"
        export PROD_DOCKER_REPO=prod-repo
        export VERSION="$VERSION" WXY="$WXY" WX="$WX" RELEASED="$RELEASED"
        bash "$NARROW"
    ) >"${WORK}/narrow.log" 2>&1
}

check_narrow() {
    local name=$1 key=$2 want=$3 got
    got="$(out_of "${WORK}/narrow.out" "$key")"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: ${key}=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/narrow.log")"
    fi
}

# Still the tip of everything: every span is this version's.
VERSION=8.2.0.0 WXY=8.2.0 WX=8.2 RELEASED="7.2.0.20 8.0.0.20 8.1.2.5 8.2.0.0"
run_narrow
check_narrow "the tip claims every span" prod_tags "8.2.0.0 8.2.0 8.2 latest"
check_narrow "the Docker Hub subset drops w.x.y" hub_tags "8.2.0.0 8.2 latest"

# The window this whole mechanism exists for, now much wider: the gap is no longer one
# run, it is however long the PROD promotion takes. A release landing in it takes the
# shared spans, and this version keeps only the tag that names it.
VERSION=8.2.0.0 WXY=8.2.0 WX=8.2 RELEASED="7.2.0.20 8.0.0.20 8.1.2.5 8.2.0.0 8.2.0.1"
run_narrow
check_narrow "a release landing in the gap takes the shared tags" prod_tags "8.2.0.0"
check_narrow "and the Docker Hub subset with them" hub_tags "8.2.0.0"
if grep -q "a newer release now holds the span" "${WORK}/narrow.log"; then
    pass "  and the drop is reported"
else
    fail "  silently"
fi
if grep -q "Not moved" "${WORK}/narrow.summary"; then
    pass "  in the run summary, not just the log"
else
    fail "  but not in the run summary"
fi

# The gates nest, so only the narrower span survives.
VERSION=8.1.2.5 WXY=8.1.2 WX=8.1 RELEASED="8.1.2.5 8.1.3.0 8.2.0.0"
run_narrow
check_narrow "a newer patch line drops w.x and latest but keeps w.x.y" prod_tags "8.1.2.5 8.1.2"
check_narrow "and the Hub subset keeps only the immutable tag" hub_tags "8.1.2.5"

# sort -V ordering, not lexical: 8.1.10.0 is newer than 8.1.2.5.
VERSION=8.1.2.5 WXY=8.1.2 WX=8.1 RELEASED="8.1.2.5 8.1.10.0"
run_narrow
check_narrow "8.1.10.0 outranks 8.1.2.5" prod_tags "8.1.2.5 8.1.2"

# w.x.y.z names exactly this version, so it is never dropped however stale the rest is.
VERSION=7.2.0.19 WXY=7.2.0 WX=7.2 RELEASED="7.2.0.19 7.2.0.20 8.2.0.0"
run_narrow
check_narrow "the version's own tag always moves" prod_tags "7.2.0.19"

# No tip means no basis for moving a shared tag on a public registry.
VERSION=8.1.2.5 WXY=8.1.2 WX=8.1 RELEASED=""
if run_narrow; then
    fail "an empty release list still moved tags"
elif grep -q "refusing to move a shared tag" "${WORK}/narrow.log"; then
    pass "an empty release list refuses to move anything"
else
    fail "an empty release list failed without naming the reason"
fi

###############################################################################
# refresh-docker-image-version.yaml :: Compare the rebuilt images against the published ones
###############################################################################
#
# This step decides whether a refresh publishes at all, so every branch of it is
# mutation-covered.

CMP="${WORK}/compare.sh"
block "$WF_VERSION" "Compare the rebuilt images against the published ones" >"$CMP"

CWS="${WORK}/cws"
mkdir -p "${CWS}/.github/bin"
# PKGS_<which> supplies an installed set per ref; EXISTS_MAP maps a ref substring to an
# image_exists.sh exit code.
cat >"${CWS}/.github/bin/image_packages.sh" <<'STUB'
#!/usr/bin/env bash
case "$2" in
*:8.1.2.5) printf '%s\n' "${PKGS_OLD}" ;;
*) printf '%s\n' "${PKGS_NEW}" ;;
esac
STUB
cat >"${CWS}/.github/bin/image_exists.sh" <<'STUB'
#!/usr/bin/env bash
while read -r pat ans; do
    [[ -n "$pat" ]] || continue
    [[ "$1" == *"$pat"* ]] || continue
    exit "$ans"
done <<<"${EXISTS_MAP:-}"
exit 0
STUB
chmod +x "${CWS}/.github/bin/image_packages.sh" "${CWS}/.github/bin/image_exists.sh"

SRV_OLD="aerospike-server 8.1.2.5-1"
SET_BASE=$'ca-certificates 20240203\ncurl 8.5.0-2\nlibssl3t64 3.0.13-0ubuntu3.15\nprocps 2:4.0.4-4'

run_cmp() {
    local out="${WORK}/cmp.out"
    : >"$out"
    (
        set +u
        export GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="${WORK}/cmp.summary"
        export GITHUB_WORKSPACE="$CWS"
        export REGISTRY=artifact.example PROD_DOCKER_REPO=prod-repo DEB_REPO=deb-repo
        export VERSION=8.1.2.5
        export COMMUNITY_TAG=reg/aerospike-server:8.1.2.5-1_T
        export ENTERPRISE_TAG=reg/aerospike-server-enterprise:8.1.2.5-1_T
        export FEDERAL_TAG=reg/aerospike-server-federal:8.1.2.5-1_T
        export FORCE_PUBLISH="${FORCE_PUBLISH:-false}" ALLOW_FIRST_PUBLISH="${ALLOW_FIRST_PUBLISH:-false}"
        export PKGS_OLD="$PKGS_OLD" PKGS_NEW="$PKGS_NEW" EXISTS_MAP="${EXISTS_MAP:-}"
        bash "$CMP"
    ) >"${WORK}/cmp.log" 2>&1
}

cmp_case() {
    unset FORCE_PUBLISH ALLOW_FIRST_PUBLISH EXISTS_MAP
    PKGS_OLD="${SRV_OLD}"$'\n'"${SET_BASE}"
    PKGS_NEW="${SRV_OLD}"$'\n'"${SET_BASE}"
    : >"${WORK}/cmp.summary"
}

check_cmp() {
    local name=$1 key=$2 want=$3 got
    got="$(out_of "${WORK}/cmp.out" "$key")"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: ${key}=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/cmp.log")"
    fi
}

expect_cmp_fail() {
    local name=$1 want=$2
    if run_cmp; then
        fail "${name}: succeeded, expected a refusal"
    elif ! grep -qF "$want" "${WORK}/cmp.log"; then
        fail "${name}: error did not mention '${want}': $(tail -2 "${WORK}/cmp.log")"
    else
        pass "$name"
    fi
}

cmp_case
run_cmp
check_cmp "identical images do not publish" publish false
check_cmp "  and are reported unchanged" changed false

cmp_case
PKGS_NEW="${SRV_OLD}"$'\nca-certificates 20240203\ncurl 8.5.0-2\nlibssl3t64 3.0.13-0ubuntu3.16\nprocps 2:4.0.4-4'
run_cmp
check_cmp "a patched OS package publishes" publish true
check_cmp "  and is reported changed" changed true

# force_publish is the lever for a recipe change that moves no package at all.
cmp_case
FORCE_PUBLISH=true
run_cmp
check_cmp "force_publish publishes an identical image" publish true
check_cmp "  while still reporting nothing changed" changed false

# A REFRESH MUST NEVER CHANGE THE SERVER BUILD. `context` stages the newest -n revision,
# so a re-spin landing in the deb repo after the image shipped would otherwise be read as
# "changed" and published under the released w.x.y.z / w.x / latest.
cmp_case
PKGS_NEW="aerospike-server 8.1.2.5-2"$'\n'"${SET_BASE}"
expect_cmp_fail "a different server revision is refused" "would ship a DIFFERENT server build"

cmp_case
PKGS_OLD="${SET_BASE}"
expect_cmp_fail "an installed set with no server package is refused" "could not find the aerospike-server package"

# 2 from image_exists.sh is "the registry answered: not there".
cmp_case
EXISTS_MAP="aerospike-server:8.1.2.5 2"
expect_cmp_fail "an absent published image is refused by default" "A refresh only re-publishes an existing release"

cmp_case
EXISTS_MAP="aerospike-server:8.1.2.5 2"
ALLOW_FIRST_PUBLISH=true
run_cmp
check_cmp "allow_first_publish treats absence as new" publish true

# 1 is "could not tell" -- a 401, 429, 5xx. Publishing on that would be unverified.
cmp_case
EXISTS_MAP="aerospike-server:8.1.2.5 1"
expect_cmp_fail "an unreadable published image is refused" "could not determine whether"

###############################################################################
# refresh-docker-image-version.yaml :: Gate
###############################################################################

GATE="${WORK}/gate.sh"
block "$WF_VERSION" "Gate" >"$GATE"

run_gate() {
    local out="${WORK}/gate.out"
    : >"$out"
    (
        set +u
        export GITHUB_OUTPUT="$out"
        export BUILD_RESULTS="${BUILD_RESULTS:-success success success}"
        export TEST_RESULT="${TEST_RESULT:-success}" VERIFY_RESULT="${VERIFY_RESULT:-success}"
        export VERIFY_PUBLISH="${VERIFY_PUBLISH:-true}"
        export PUSH_REQUESTED="${PUSH_REQUESTED:-true}"
        export COMMUNITY_DIGEST="${COMMUNITY_DIGEST-sha256:a}" ENTERPRISE_DIGEST="${ENTERPRISE_DIGEST-sha256:b}"
        export FEDERAL_DIGEST="${FEDERAL_DIGEST-sha256:c}"
        export COMMUNITY_TAG="${COMMUNITY_TAG-r/c:8.1.2.5-1_20261006T000000Z}"
        export ENTERPRISE_TAG="${ENTERPRISE_TAG-r/e:8.1.2.5-1_20261006T000000Z}"
        export FEDERAL_TAG="${FEDERAL_TAG-r/f:8.1.2.5-1_20261006T000000Z}"
        bash "$GATE"
    ) >"${WORK}/gate.log" 2>&1
}

gate_case() {
    unset BUILD_RESULTS TEST_RESULT VERIFY_RESULT VERIFY_PUBLISH PUSH_REQUESTED \
        COMMUNITY_DIGEST ENTERPRISE_DIGEST FEDERAL_DIGEST \
        COMMUNITY_TAG ENTERPRISE_TAG FEDERAL_TAG
}

check_gate() {
    local name=$1 key=$2 want=$3 got
    got="$(out_of "${WORK}/gate.out" "$key")"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: ${key}=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/gate.log")"
    fi
}

expect_gate_fail() {
    local name=$1 want=$2
    if run_gate; then
        fail "${name}: succeeded, expected a refusal"
    elif ! grep -qF "$want" "${WORK}/gate.log"; then
        fail "${name}: error did not mention '${want}': $(tail -2 "${WORK}/gate.log")"
    else
        pass "$name"
    fi
}

gate_case
run_gate
check_gate "a complete push run publishes" publish true
check_gate "  with the bundle coordinate derived from the enterprise tag" bundle_version 8.1.2.5-1_20261006T000000Z

# verify-refresh is the ONLY thing that compared the rebuild against what is published.
# Gated out of a run that is about to bundle and promote, it must stop the run.
gate_case
VERIFY_RESULT=skipped
expect_gate_fail "a skipped verify-refresh on a push run is refused" "never compared against the published ones"
gate_case
VERIFY_RESULT=failure
expect_gate_fail "a failed verify-refresh is refused" "never compared against the published ones"

gate_case
VERIFY_PUBLISH=false TEST_RESULT=skipped
run_gate
check_gate "nothing to publish allows a skipped test-images" publish false

# But a skipped test on a run that IS about to publish means shipping untested images.
gate_case
TEST_RESULT=skipped
expect_gate_fail "a skipped test-images on a publishing run is refused" "about to publish"

gate_case
BUILD_RESULTS="success failure success"
expect_gate_fail "a failed build leg is refused" "not bundling or moving any tag"

# A dry run: no digest is expected, but the tests still had to run -- that is the only
# thing exercising the generated context before a real dispatch.
gate_case
ENTERPRISE_DIGEST='' PUSH_REQUESTED=false VERIFY_RESULT=skipped
run_gate
check_gate "a dry run reports nothing to publish" publish false
gate_case
ENTERPRISE_DIGEST='' PUSH_REQUESTED=false VERIFY_RESULT=skipped TEST_RESULT=skipped
expect_gate_fail "a dry run with skipped tests is refused" "the generated contexts were never exercised"

# push_docker_jfrog=true with no digest means the manifest was never published.
gate_case
ENTERPRISE_DIGEST=''
expect_gate_fail "a push run with no digest is refused" "nothing was pushed"

# Enterprise pushed, so every edition must have: a gap here would surface mid-retag.
gate_case
FEDERAL_DIGEST=''
expect_gate_fail "a missing edition digest is refused" "produced no digest/immutable tag"

gate_case
ENTERPRISE_TAG=r/e:not-a-coordinate
expect_gate_fail "an unparseable immutable tag is refused" "cannot derive a bundle version"

###############################################################################
# refresh-docker-images.yaml :: Resolve docker_repo_ref
###############################################################################

RREF="${WORK}/resolve_ref.sh"
block "$WF_DISPATCHER" "Resolve docker_repo_ref" >"$RREF"

RWS="${WORK}/rws"
mkdir -p "${RWS}/.github/bin"
cat >"${RWS}/.github/bin/docker_repo_tip.sh" <<'STUB'
#!/usr/bin/env bash
[[ "${TIP_FAILS:-false}" != true ]] || exit 1
printf '%s\n' "${TIP:-dddddddddddddddddddddddddddddddddddddddd}"
STUB
chmod +x "${RWS}/.github/bin/docker_repo_tip.sh"

PIN=d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6

run_rref() {
    local out="${WORK}/rref.out"
    : >"$out"
    (
        set +u
        cd "$RWS" || exit 1
        export GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="${WORK}/rref.summary"
        export REQUESTED="$REQUESTED" TIP="${TIP:-}" TIP_FAILS="${TIP_FAILS:-false}"
        bash "$RREF"
    ) >"${WORK}/rref.log" 2>&1
}

rref_case() {
    unset TIP TIP_FAILS
    : >"${WORK}/rref.summary"
}

check_rref() {
    local name=$1 want=$2 got
    got="$(out_of "${WORK}/rref.out" docker_repo_ref)"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: docker_repo_ref=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/rref.log")"
    fi
}

# The whole point: the sentinel becomes a sha HERE, so the three independent checkouts
# downstream cannot land on different commits.
rref_case
REQUESTED=master TIP=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
run_rref
check_rref "the sentinel resolves to a sha" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
if grep -q "every job in this fan-out uses that commit" "${WORK}/rref.log"; then
    pass "  and says the fan-out is pinned to it"
else
    fail "  without saying the fan-out is pinned"
fi

rref_case
REQUESTED="$PIN" TIP="$PIN"
run_rref
check_rref "an explicit sha passes through untouched" "$PIN"
if grep -q "is master's tip" "${WORK}/rref.log"; then
    pass "  and a pin that is the tip says so"
else
    fail "  without reporting that it is current"
fi

# The pin goes stale silently otherwise. Visibility, not adoption: the resolved ref is
# still the pin.
rref_case
REQUESTED="$PIN" TIP=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_rref
check_rref "a stale pin is still what gets built" "$PIN"
if grep -q "master is now bbbbbbbb" "${WORK}/rref.log"; then
    pass "  and the drift is reported"
else
    fail "  silently"
fi
if grep -q "master is now" "${WORK}/rref.summary"; then
    pass "  in the run summary, not just the log"
else
    fail "  but not in the run summary"
fi

# A GitHub outage must not fail a run that already has its sha.
rref_case
REQUESTED="$PIN" TIP_FAILS=true
run_rref
check_rref "an unreachable tip does not fail a pinned run" "$PIN"
if grep -q "not reporting pin drift" "${WORK}/rref.log"; then
    pass "  and the missing drift check is reported"
else
    fail "  silently"
fi

# But a run that ASKED for the tip has nothing to fall back to.
rref_case
REQUESTED=master TIP_FAILS=true
if run_rref; then
    fail "docker_repo_ref=master succeeded with an unresolvable tip"
elif grep -q "tip could not be resolved" "${WORK}/rref.log"; then
    pass "an unreachable tip fails a run that asked for it"
else
    fail "an unreachable tip failed without naming the reason"
fi

###############################################################################
# refresh-docker-image-version.yaml :: Validate version against the release tag history
###############################################################################
#
# This workflow is directly dispatchable, so it carries its own copy of the release-ref
# policy rather than relying on the dispatcher having applied one.

VAL="${WORK}/validate.sh"
block "$WF_VERSION" "Validate version against the release tag history" >"$VAL"

VWS2="${WORK}/vws2"
mkdir -p "${VWS2}/bin" "${VWS2}/.github/bin"
cat >"${VWS2}/.github/bin/docker_repo_tip.sh" <<'STUB'
#!/usr/bin/env bash
[[ "${TIP_FAILS:-false}" != true ]] || exit 1
printf '%s\n' "${TIP:-dddddddddddddddddddddddddddddddddddddddd}"
STUB
chmod +x "${VWS2}/.github/bin/docker_repo_tip.sh"
cat >"${VWS2}/bin/git" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "${GIT_TAGS:-}"
STUB
chmod +x "${VWS2}/bin/git"

validate_case() {
    unset TIP TIP_FAILS DOCKER_REPO_REF REF_NAME GIT_TAGS \
        IN_PUSH_DOCKER_JFROG IN_PROMOTE_BUNDLE IN_FORCE_PUBLISH IN_ALLOW_FIRST_PUBLISH
}

run_validate() {
    local out="${WORK}/validate.out"
    : >"$out"
    (
        set +u
        cd "$VWS2" || exit 1
        export GITHUB_OUTPUT="$out" PATH="${VWS2}/bin:${PATH}"
        export GIT_TAGS="${GIT_TAGS-v${VERSION}}"
        export VERSION="${VERSION}" ASADM_VERSION=""
        export TIP="${TIP:-}" TIP_FAILS="${TIP_FAILS:-false}"
        export DOCKER_REPO_REF="${DOCKER_REPO_REF:-d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6}"
        export REF_NAME="${REF_NAME:-master}"
        export IN_PUSH_DOCKER_JFROG="${IN_PUSH_DOCKER_JFROG:-true}"
        export IN_PROMOTE_BUNDLE="${IN_PROMOTE_BUNDLE:-true}"
        export IN_FORCE_PUBLISH="${IN_FORCE_PUBLISH:-false}"
        export IN_ALLOW_FIRST_PUBLISH="${IN_ALLOW_FIRST_PUBLISH:-false}"
        bash "$VAL"
    ) >"${WORK}/validate.log" 2>&1
}

check_validate() {
    local name=$1 key=$2 want=$3 got
    got="$(out_of "${WORK}/validate.out" "$key")"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: ${key}=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/validate.log")"
    fi
}

validate_case
VERSION=8.1.2.5 REF_NAME=master run_validate
check_validate "master pushes" push_docker_jfrog true
check_validate "master promotes" promote_bundle true
check_validate "the distro is resolved from the lineage" distro ubuntu24.04

validate_case
VERSION=8.1.2.5 REF_NAME=hotfix/8.1.2 run_validate
check_validate "a hotfix line promotes" promote_bundle true

# Degrade, never upgrade: an off-ref run cannot be talked into publishing, and an on-ref
# run still honours a deliberate false.
validate_case
VERSION=8.1.2.5 REF_NAME=master IN_PROMOTE_BUNDLE=false run_validate
check_validate "a deliberate false survives on a release ref" promote_bundle false

# This workflow is directly dispatchable, so it resolves the sentinel itself rather than
# assuming the dispatcher already did. `context` and `test-images` read the resolved
# output, so their two checkouts are the same commit by construction.
validate_case
VERSION=8.1.2.5 DOCKER_REPO_REF=master TIP=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa run_validate
check_validate "the master sentinel resolves to a sha" docker_repo_ref aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

validate_case
VERSION=8.1.2.5 DOCKER_REPO_REF=d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6 run_validate
check_validate "an explicit sha passes through" docker_repo_ref d2a0368abedf9aa216f5cd7c7e97d86b0111cdd6

expect_validate_error() {
    local name=$1 want=$2
    if run_validate; then
        fail "${name}: accepted, expected a refusal"
    elif ! grep -qF "$want" "${WORK}/validate.log"; then
        fail "${name}: error did not mention '${want}': $(tail -2 "${WORK}/validate.log")"
    else
        pass "$name"
    fi
}

# The ship tag is the record that a version was released; without this check the
# workflow would refresh something that never shipped.
validate_case
VERSION=8.1.2.5 GIT_TAGS='' \
    expect_validate_error "a version with no ship tag is refused" "is not a release tag"

validate_case
VERSION=8.1.2.5 DOCKER_REPO_REF=some/branch \
    expect_validate_error "a branch name is refused here too" "or the literal 'master'"
validate_case
VERSION=8.1.2.5 DOCKER_REPO_REF=master TIP_FAILS=true \
    expect_validate_error "an unresolvable tip fails the run" "tip could not be resolved"

# This workflow is directly dispatchable, so it carries the release-ref policy itself
# rather than relying on the dispatcher. It REFUSES rather than degrading -- quietly
# turning an asked-for publish into a dry run is the kind of thing nobody reads the
# notice about.
validate_case
VERSION=8.1.2.5 REF_NAME=pvinh/SERVER-1520-refresh-docker-images \
    expect_validate_error "a branch dispatch asking to publish is refused" "is not a release ref"
validate_case
VERSION=8.1.2.5 REF_NAME=preview/8.3 \
    expect_validate_error "preview/ is not a release ref here either" "is not a release ref"

# Each offending toggle is named, so the operator knows what to unset.
validate_case
VERSION=8.1.2.5 REF_NAME=some/branch IN_PUSH_DOCKER_JFROG=false IN_PROMOTE_BUNDLE=false \
    IN_FORCE_PUBLISH=true \
    expect_validate_error "force_publish alone is enough to refuse" "force_publish"
validate_case
VERSION=8.1.2.5 REF_NAME=some/branch IN_PUSH_DOCKER_JFROG=false IN_PROMOTE_BUNDLE=false \
    IN_ALLOW_FIRST_PUBLISH=true \
    expect_validate_error "allow_first_publish alone is enough to refuse" "allow_first_publish"

# The dispatcher's degraded children arrive with every toggle false, so they still pass
# and a branch dry run runs end to end.
validate_case
VERSION=8.1.2.5 REF_NAME=some/branch IN_PUSH_DOCKER_JFROG=false IN_PROMOTE_BUNDLE=false run_validate
check_validate "a branch dry run with everything false is accepted" push_docker_jfrog false
check_validate "  and stays a dry run" promote_bundle false

###############################################################################
# publish-refreshed-tags.yaml :: Gate the run
###############################################################################

PGATE="${WORK}/pgate.sh"
block "$WF_PUBLISH" "Gate the run" >"$PGATE"

run_pgate() {
    local out="${WORK}/pgate.out"
    : >"$out"
    (
        set +u
        export GITHUB_OUTPUT="$out"
        export ENABLED="${ENABLED:-}" EVENT="${EVENT:-workflow_dispatch}" REF_NAME="${REF_NAME:-master}"
        # Faithful to GitHub: a workflow_dispatch arrives with the declared defaults
        # filled in, a schedule with NOTHING. Defaulting these unconditionally is what
        # hid the bug where the armed cron skipped every write and still went green, so
        # the schedule case must get the empty strings a schedule really gets.
        if [[ "${EVENT:-workflow_dispatch}" == "schedule" ]]; then
            export IN_UPDATE_MUTABLE_TAGS="" IN_PUSH_DOCKER_HUB=""
        else
            export IN_UPDATE_MUTABLE_TAGS="${IN_UPDATE_MUTABLE_TAGS:-true}"
            export IN_PUSH_DOCKER_HUB="${IN_PUSH_DOCKER_HUB:-true}"
        fi
        bash "$PGATE"
    ) >"${WORK}/pgate.log" 2>&1
}

check_pgate() {
    local name=$1 key=$2 want=$3 got
    got="$(out_of "${WORK}/pgate.out" "$key")"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: ${key}=${got@Q}, want ${want@Q}"
    fi
}

pgate_case() { unset ENABLED EVENT REF_NAME IN_UPDATE_MUTABLE_TAGS IN_PUSH_DOCKER_HUB; }

pgate_case
EVENT=schedule run_pgate
check_pgate "the daily scan is disarmed with the variable unset" skip_all true

pgate_case
EVENT=schedule ENABLED=true run_pgate
check_pgate "the daily scan runs when armed" skip_all false

# THE schedule bug: a schedule event carries no inputs, so every `inputs.*` is null.
# Reading them directly made the armed cron skip the tag write, the Hub copy AND the
# Hub-parity half of the scan, and still report success -- and no dispatch rehearsal
# could show it, because a dispatch fills the defaults in.
check_pgate "  and resolves update_mutable_tags for a schedule" update_mutable_tags true
check_pgate "  and push_docker_hub for a schedule" push_docker_hub true

# A dispatch's explicit false still has to survive.
pgate_case
IN_UPDATE_MUTABLE_TAGS=false IN_PUSH_DOCKER_HUB=true run_pgate
check_pgate "a dispatch's explicit false survives" update_mutable_tags false
check_pgate "  alongside an explicit true" push_docker_hub true

pgate_case
IN_UPDATE_MUTABLE_TAGS=yes IN_PUSH_DOCKER_HUB=true run_pgate
if grep -q "update_mutable_tags resolved to 'yes'" "${WORK}/pgate.log"; then
    pass "a non-boolean toggle is refused by name"
else
    fail "a non-boolean toggle was accepted"
fi

pgate_case
ENABLED='' run_pgate
check_pgate "a dispatch is never gated by the variable" skip_all false

# The scan is read-only, so it runs anywhere; only the half that writes to PROD and
# public Docker Hub is held to a release ref.
pgate_case
REF_NAME=pvinh/SERVER-1520-refresh-docker-images run_pgate
check_pgate "a branch dispatch may scan" skip_all false
check_pgate "but may not publish" is_release_ref false

pgate_case
REF_NAME=master run_pgate
check_pgate "master may publish" is_release_ref true

pgate_case
REF_NAME=hotfix/8.1.2 run_pgate
check_pgate "a hotfix line may publish" is_release_ref true

###############################################################################
# publish-refreshed-tags.yaml :: Find refreshes PROD holds but has not published
###############################################################################

SCAN="${WORK}/scan.sh"
block "$WF_PUBLISH" "Find refreshes PROD holds but has not published" >"$SCAN"

SWS="${WORK}/sws"
mkdir -p "${SWS}/.github/bin" "${SWS}/bin"
cat >"${SWS}/.github/bin/registry_tags.sh" <<'STUB'
#!/usr/bin/env bash
# Nothing at all for an empty list: `printf '%s\n'` with no args still emits one blank
# line, which the real script's `sort -u` would never produce.
[[ -z "${PROD_TAGS:-}" ]] || printf '%s\n' ${PROD_TAGS}
STUB
# One docker stub for both halves of imagetools inspect: `.Manifest` for digests,
# `.Image` for the label that separates a refresh from a release. Maps are exact-ref.
cat >"${SWS}/bin/docker" <<'STUB'
#!/usr/bin/env bash
want_image=false ref="" prev=""
for a in "$@"; do
    [[ "$a" != *".Image"* ]] || want_image=true
    [[ "$prev" != "inspect" ]] || ref="$a"
    prev="$a"
done
if [[ "$want_image" == true ]]; then
    while read -r pat ans; do
        [[ -n "$pat" ]] || continue
        [[ "$ref" == "$pat" ]] || continue
        [[ "$ans" != "nolabel" ]] || { printf '{"linux/amd64":{"config":{"Labels":{}}}}'; exit 0; }
        printf '{"linux/amd64":{"config":{"Labels":{"com.aerospike.refresh.run":"%s"}}}}' "$ans"
        exit 0
    done <<<"${LABEL_MAP:-}"
    printf '{"linux/amd64":{"config":{"Labels":{"com.aerospike.refresh.run":"99"}}}}'
    exit 0
fi
while read -r pat ans; do
    [[ -n "$pat" ]] || continue
    [[ "$ref" == "$pat" ]] || continue
    [[ "$ans" != "absent" ]] || exit 1
    printf '{"digest":"%s"}' "$ans"
    exit 0
done <<<"${DIGEST_MAP:-}"
printf '{"digest":"sha256:unmapped"}'
STUB
# GNU `date -u -d "<n> days ago"` is what the step uses; macOS has no such flag.
cat >"${SWS}/bin/date" <<'STUB'
#!/usr/bin/env bash
printf '%s' "${FAKE_CUTOFF:-20000101T000000Z}"
STUB
chmod +x "${SWS}/.github/bin/registry_tags.sh" "${SWS}/bin/docker" "${SWS}/bin/date"

PE="artifact.example/prod-repo/aerospike-server-enterprise"
HE="docker.io/testorg/aerospike-server-enterprise"
COORD="8.1.2.5-1_20261006T174233Z"

run_scan() {
    local out="${WORK}/scan.out"
    : >"$out"
    (
        set +u
        cd "$SWS" || exit 1
        export GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="${WORK}/scan.summary"
        export PATH="${SWS}/bin:${PATH}"
        export REGISTRY=artifact.example PROD_DOCKER_REPO=prod-repo
        export HUB_ORG=testorg REFRESH_LABEL=com.aerospike.refresh.run
        export ONLY_VERSION="${ONLY_VERSION:-}" PIN="${PIN:-}" LOOKBACK_DAYS="${LOOKBACK_DAYS:-0}"
        export CHECK_HUB="${CHECK_HUB:-true}"
        export PROD_TAGS="$PROD_TAGS" DIGEST_MAP="$DIGEST_MAP" LABEL_MAP="${LABEL_MAP:-}"
        export FAKE_CUTOFF="${FAKE_CUTOFF:-}"
        bash "$SCAN"
    ) >"${WORK}/scan.log" 2>&1
}

scan_case() {
    unset ONLY_VERSION PIN LOOKBACK_DAYS CHECK_HUB FAKE_CUTOFF
    LABEL_MAP=""
    : >"${WORK}/scan.summary"
}

check_scan() {
    local name=$1 want=$2 got
    got="$(out_of "${WORK}/scan.out" versions)"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: versions=${got@Q}, want ${want@Q} -- $(tail -3 "${WORK}/scan.log")"
    fi
}

# PROD's w.x.y.z already points at the refresh, and Hub agrees: converged, say nothing.
scan_case
PROD_TAGS="8.1.2.5 8.1.2 latest ${COORD}"
DIGEST_MAP="${PE}:${COORD} sha256:new
${PE}:8.1.2.5 sha256:new
${HE}:8.1.2.5 sha256:new"
run_scan
check_scan "a fully published refresh is left alone" "[]"

# The ordinary case this exists for: PROD holds the refresh, its tags still point at the
# build it replaces.
scan_case
PROD_TAGS="8.1.2.5 ${COORD}"
DIGEST_MAP="${PE}:${COORD} sha256:new
${PE}:8.1.2.5 sha256:old
${HE}:8.1.2.5 sha256:old"
run_scan
check_scan "an unpublished refresh is selected" '["8.1.2.5"]'

# THE critical case. A release between its PROD promotion and whatever tags it looks
# exactly like an unpublished refresh -- newest immutable tag, stale mutable tag. Only
# the label separates them, and acting here would mean taking over PROD tagging for the
# release process.
scan_case
PROD_TAGS="8.1.2.5 ${COORD}"
DIGEST_MAP="${PE}:${COORD} sha256:new
${PE}:8.1.2.5 sha256:old
${HE}:8.1.2.5 sha256:old"
LABEL_MAP="${PE}:${COORD} nolabel"
run_scan
check_scan "an unlabelled image is left to the release process" "[]"
if grep -q "not a refresh" "${WORK}/scan.log"; then
    pass "  and the reason is logged"
else
    fail "  without saying why"
fi

# PROD converged, Hub behind: otherwise permanent, since nothing upstream re-derives it
# once PROD looks right.
scan_case
PROD_TAGS="8.1.2.5 ${COORD}"
DIGEST_MAP="${PE}:${COORD} sha256:new
${PE}:8.1.2.5 sha256:new
${HE}:8.1.2.5 sha256:old"
run_scan
check_scan "Docker Hub lagging PROD is selected" '["8.1.2.5"]'

scan_case
PROD_TAGS="8.1.2.5 ${COORD}"
DIGEST_MAP="${PE}:${COORD} sha256:new
${PE}:8.1.2.5 sha256:new
${HE}:8.1.2.5 absent"
run_scan
check_scan "a version absent from Docker Hub is selected" '["8.1.2.5"]'

# push_docker_hub=false means Hub is not this run's business.
scan_case
CHECK_HUB=false
PROD_TAGS="8.1.2.5 ${COORD}"
DIGEST_MAP="${PE}:${COORD} sha256:new
${PE}:8.1.2.5 sha256:new
${HE}:8.1.2.5 sha256:old"
run_scan
check_scan "Hub is not consulted when push_docker_hub is false" "[]"

# Several versions, newest coordinate per version.
scan_case
PROD_TAGS="8.1.2.5 8.0.0.20 ${COORD} 8.1.2.5-1_20260101T000000Z 8.0.0.20-1_20261006T120000Z"
DIGEST_MAP="${PE}:${COORD} sha256:new1
${PE}:8.1.2.5 sha256:old1
${HE}:8.1.2.5 sha256:old1
${PE}:8.0.0.20-1_20261006T120000Z sha256:new2
${PE}:8.0.0.20 sha256:old2
${HE}:8.0.0.20 sha256:old2"
run_scan
check_scan "every unpublished version is selected, version-sorted" '["8.0.0.20","8.1.2.5"]'

# Only release-shaped tags: there is no refresh anywhere, which is the normal state most
# days and must not be an error.
scan_case
PROD_TAGS="8.1.2.5 8.1.2 8.1 latest"
DIGEST_MAP=""
run_scan
check_scan "a repo with no refresh coordinates is quiet" "[]"

# An empty listing is not "nothing to do" -- the repo always has tags, so an empty answer
# means the listing was read wrong.
scan_case
PROD_TAGS=""
DIGEST_MAP=""
if run_scan; then
    fail "an empty tag listing was treated as nothing to do"
elif grep -q "listed no tags at all" "${WORK}/scan.log"; then
    pass "an empty tag listing fails closed"
else
    fail "an empty tag listing failed without naming the reason"
fi

# The lookback bounds the scan and keeps a months-old unpublished refresh a thing a human
# looks at rather than something this quietly ships.
scan_case
LOOKBACK_DAYS=30
FAKE_CUTOFF=20261001T000000Z
PROD_TAGS="8.1.2.5 8.1.2.5-1_20260101T000000Z"
DIGEST_MAP="${PE}:8.1.2.5-1_20260101T000000Z sha256:new
${PE}:8.1.2.5 sha256:old
${HE}:8.1.2.5 sha256:old"
run_scan
check_scan "a refresh older than the lookback is not reconciled" "[]"

scan_case
LOOKBACK_DAYS=30
FAKE_CUTOFF=20261001T000000Z
PROD_TAGS="8.1.2.5 ${COORD}"
DIGEST_MAP="${PE}:${COORD} sha256:new
${PE}:8.1.2.5 sha256:old
${HE}:8.1.2.5 sha256:old"
run_scan
check_scan "a refresh inside the lookback is reconciled" '["8.1.2.5"]'

# An explicit version overrides the lookback: the operator asked for it by name.
scan_case
ONLY_VERSION=8.1.2.5
LOOKBACK_DAYS=30
FAKE_CUTOFF=20261001T000000Z
PROD_TAGS="8.1.2.5 8.1.2.5-1_20260101T000000Z"
DIGEST_MAP="${PE}:8.1.2.5-1_20260101T000000Z sha256:new
${PE}:8.1.2.5 sha256:old
${HE}:8.1.2.5 sha256:old"
run_scan
check_scan "an explicit version ignores the lookback" '["8.1.2.5"]'

scan_case
ONLY_VERSION=8.1.2.5
PROD_TAGS="8.0.0.20 8.0.0.20-1_20261006T120000Z"
DIGEST_MAP=""
if run_scan; then
    fail "an explicit version with no refresh in PROD succeeded"
elif grep -q "holds no refreshed" "${WORK}/scan.log"; then
    pass "an explicit version with no refresh in PROD is an error"
else
    fail "an explicit version with no refresh failed without naming the reason"
fi

scan_case
ONLY_VERSION=8.1
PROD_TAGS="8.1.2.5 ${COORD}"
DIGEST_MAP=""
if run_scan; then
    fail "a lineage was accepted as a version"
elif grep -q "is not a w.x.y.z version" "${WORK}/scan.log"; then
    pass "a lineage is not a version here"
else
    fail "a lineage failed without naming the reason"
fi

###############################################################################
# publish-refreshed-tags.yaml :: Resolve the refreshed images in PROD
###############################################################################

FIND="${WORK}/find.sh"
block "$WF_PUBLISH" "Resolve the refreshed images in PROD" >"$FIND"

FWS="${WORK}/fws"
mkdir -p "${FWS}/.github/bin" "${FWS}/bin"
cat >"${FWS}/.github/bin/registry_tags.sh" <<'STUB'
#!/usr/bin/env bash
case "$2" in
aerospike-server-enterprise) printf '%s\n' ${TAGS_ENT:-} ;;
aerospike-server-federal) printf '%s\n' ${TAGS_FED:-} ;;
*) printf '%s\n' ${TAGS_CE:-} ;;
esac
STUB
cat >"${FWS}/bin/docker" <<'STUB'
#!/usr/bin/env bash
for a in "$@"; do
    if [[ "$a" == *".Image"* ]]; then
        [[ "${HAS_LABEL:-true}" == true ]] || { printf '{"linux/amd64":{"config":{"Labels":{}}}}'; exit 0; }
        printf '{"linux/amd64":{"config":{"Labels":{"com.aerospike.refresh.run":"42"}}}}'
        exit 0
    fi
done
printf '{"digest":"sha256:%s"}' "${DIGEST:-deadbeef}"
STUB
cat >"${FWS}/bin/curl" <<'STUB'
#!/usr/bin/env bash
out=""
while [[ $# -gt 0 ]]; do
    case "$1" in
    -o) out="$2"; shift 2 ;;
    *) shift ;;
    esac
done
[[ -z "$out" ]] || printf '%s' "${CURL_BODY:-}" >"$out"
printf '%s' "${CURL_CODE:-200}"
STUB
chmod +x "${FWS}/.github/bin/registry_tags.sh" "${FWS}/bin/docker" "${FWS}/bin/curl"

PROMOTED_PROD='{"promotions":[{"environment":"TEST","status":"COMPLETED"},{"environment":"STAGE","status":"COMPLETED"},{"environment":"PROD","status":"COMPLETED"}]}'
STAGE_ONLY='{"promotions":[{"environment":"TEST","status":"COMPLETED"},{"environment":"STAGE","status":"COMPLETED"}]}'

run_find() {
    local out="${WORK}/find.out"
    : >"$out"
    (
        set +u
        cd "$FWS" || exit 1
        export GITHUB_OUTPUT="$out" GITHUB_STEP_SUMMARY="${WORK}/find.summary"
        export PATH="${FWS}/bin:${PATH}"
        export REGISTRY=artifact.example PROD_DOCKER_REPO=prod-repo
        export JFROG_URL=https://jfrog.example JFROG_PROJECT=database
        export REFRESH_LABEL=com.aerospike.refresh.run
        export RELEASE_BUNDLE_NAME=aerospike-server-docker JF_TOKEN=t
        export VERSION="${VERSION}" PIN="${PIN:-}" ONLY_VERSION="${ONLY_VERSION:-}"
        export TAGS_CE="$TAGS_CE" TAGS_ENT="$TAGS_ENT" TAGS_FED="$TAGS_FED"
        export CURL_BODY="${CURL_BODY}" CURL_CODE="${CURL_CODE:-200}"
        export HAS_LABEL="${HAS_LABEL:-true}"
        bash "$FIND"
    ) >"${WORK}/find.log" 2>&1
}

find_case() {
    unset PIN CURL_CODE ONLY_VERSION HAS_LABEL
    VERSION=8.1.2.5
    TAGS_CE="8.1.2.5 8.1.2 latest 8.1.2.5-1_20261006T174200Z"
    TAGS_ENT="8.1.2.5 ${COORD}"
    TAGS_FED="8.1.2.5 8.1.2.5-1_20261006T174250Z"
    CURL_BODY="$PROMOTED_PROD"
}

check_find() {
    local name=$1 key=$2 want=$3 got
    got="$(out_of "${WORK}/find.out" "$key")"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: ${key}=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/find.log")"
    fi
}

expect_find_fail() {
    local name=$1 want=$2
    if run_find; then
        fail "${name}: succeeded, expected a refusal"
    elif ! grep -qF "$want" "${WORK}/find.log"; then
        fail "${name}: error did not mention '${want}': $(tail -2 "${WORK}/find.log")"
    else
        pass "$name"
    fi
}

find_case
run_find
check_find "the bundle version is enterprise's coordinate" bundle_version "$COORD"
check_find "each edition resolves its own timestamp (community)" community_tag 8.1.2.5-1_20261006T174200Z
check_find "each edition resolves its own timestamp (federal)" federal_tag 8.1.2.5-1_20261006T174250Z
check_find "the patch line is derived" wxy 8.1.2
check_find "the lineage is derived" wx 8.1

# Re-checked at the step that writes, not inherited from the scan.
find_case
HAS_LABEL=false
expect_find_fail "an unlabelled coordinate refuses to take PROD tags" "was not produced by a refresh"

find_case
CURL_BODY="$STAGE_ONLY"
expect_find_fail "a bundle only in STAGE refuses to publish" "is not promoted to PROD"

find_case
CURL_CODE=404 CURL_BODY='{}'
expect_find_fail "no promotion record at all refuses to publish" "returned HTTP 404"

# Reaching PROD is not the same as completing there: a FAILED promotion leaves the
# record present with the images absent or half-copied.
find_case
CURL_BODY='{"promotions":[{"environment":"TEST","status":"COMPLETED"},{"environment":"PROD","status":"FAILED"}]}'
expect_find_fail "a FAILED PROD promotion refuses to publish" "is not promoted to PROD"
find_case
CURL_BODY='{"promotions":[{"environment":"PROD","status":"IN_PROGRESS"}]}'
expect_find_fail "an in-progress PROD promotion refuses to publish" "is not promoted to PROD"

find_case
TAGS_ENT="8.1.2.5 8.1.2"
expect_find_fail "a version with no refreshed image refuses" "no refreshed aerospike-server-enterprise image"

find_case
TAGS_FED="8.1.2.5"
expect_find_fail "a missing edition refuses" "the three editions must ship as a set"

find_case
TAGS_ENT="8.1.2.5-1_20260101T000000Z ${COORD}"
run_find
check_find "the newest refresh wins by default" bundle_version "$COORD"

# A pin only means anything for the version it names: the matrix runs this step once per
# version, and a pin meant for one must not be applied to another.
find_case
ONLY_VERSION=8.1.2.5 PIN=8.1.2.5-1_20260101T000000Z
TAGS_ENT="8.1.2.5-1_20260101T000000Z ${COORD}"
run_find
check_find "bundle_version pins an older refresh" bundle_version 8.1.2.5-1_20260101T000000Z

find_case
ONLY_VERSION=8.0.0.20 PIN=8.0.0.20-1_20260101T000000Z
TAGS_ENT="8.1.2.5-1_20260101T000000Z ${COORD}"
run_find
check_find "a pin for another version is ignored" bundle_version "$COORD"

find_case
ONLY_VERSION=8.1.2.5 PIN=8.1.2.5-1_19990101T000000Z
expect_find_fail "a pin PROD does not hold refuses" "has no aerospike-server-enterprise"

find_case
ONLY_VERSION=8.1.2.5 PIN=not-a-coordinate
expect_find_fail "a malformed pin refuses" "is not a <8.1.2.5-n>_<timestamp> coordinate"

find_case
VERSION=$'8.1.2.5\nversion=9.9.9.9'
expect_find_fail "a newline in version refuses" "is not a w.x.y.z version"

###############################################################################
# refresh-docker-image-version.yaml :: Does this release bundle version already exist?
###############################################################################

BPC="${WORK}/bundle-precheck.sh"
block "$WF_VERSION" "Does this release bundle version already exist?" >"$BPC"

BWS="${WORK}/bws"
mkdir -p "$BWS"
cat >"${BWS}/jf" <<'STUB'
#!/usr/bin/env bash
[[ -z "${JF_OUT:-}" ]] || printf '%s' "$JF_OUT"
exit "${JF_RC:-0}"
STUB
chmod +x "${BWS}/jf"

run_bpc() {
    local out="${WORK}/bpc.out"
    : >"$out"
    (
        set +u
        export GITHUB_OUTPUT="$out" PATH="${BWS}:${PATH}"
        export BUNDLE_NAME=aerospike-server-docker JFROG_PROJECT=database
        export BUNDLE_VERSION="${BUNDLE_VERSION:-8.1.2.5-1_20261006T174233Z}"
        export JF_OUT="$JF_OUT" JF_RC="${JF_RC:-0}"
        bash "$BPC"
    ) >"${WORK}/bpc.log" 2>&1
}

bpc_case() {
    unset BUNDLE_VERSION JF_RC
    JF_OUT=""
}

check_bpc() {
    local name=$1 want=$2 got
    got="$(out_of "${WORK}/bpc.out" bundle_exists)"
    if [[ "$got" == "$want" ]]; then
        pass "$name"
    else
        fail "${name}: bundle_exists=${got@Q}, want ${want@Q} -- $(tail -2 "${WORK}/bpc.log")"
    fi
}

expect_bpc_fail() {
    local name=$1 want=$2
    if run_bpc; then
        fail "${name}: succeeded, expected a refusal"
    elif ! grep -qF "$want" "${WORK}/bpc.log"; then
        fail "${name}: error did not mention '${want}': $(tail -2 "${WORK}/bpc.log")"
    else
        pass "$name"
    fi
}

# A bundle name that exists but has no versions yet answers exit 0 with an empty array.
# That is a genuine "not found", not an error.
bpc_case
JF_OUT='{"release_bundles":[],"total":0}'
run_bpc
check_bpc "an empty version list means the coordinate is free" false

# The namespace is shared with the release path, so other versions are always present.
bpc_case
JF_OUT='{"release_bundles":[{"release_bundle_version":"8.1.2.5-1_20260101T000000Z"},{"release_bundle_version":"8.2.0.0-1"}],"total":2}'
run_bpc
check_bpc "another version is not this one" false

# The reachable collision: Re-run failed jobs replays the image jobs' immutable-tag
# outputs, so the same coordinate is asked for twice. Creation must be skipped, not
# re-stamped, and promotion must still happen.
bpc_case
JF_OUT='{"release_bundles":[{"release_bundle_version":"8.1.2.5-1_20260101T000000Z"},{"release_bundle_version":"8.1.2.5-1_20261006T174233Z"}],"total":2}'
run_bpc
check_bpc "this exact coordinate is found" true

# Everything below is the fail-closed half. Reporting "no bundle" on any of these would
# send the non-idempotent create at a coordinate that may already be taken.
bpc_case
JF_RC=1 JF_OUT=''
expect_bpc_fail "a failed search fails closed" "failing closed"

bpc_case
JF_OUT='not json at all'
expect_bpc_fail "a malformed response fails closed" "failing closed"

# release_bundles: null is the one has() accepts and .release_bundles[]? then swallows,
# so the probe would examine nothing and report "free".
bpc_case
JF_OUT='{"release_bundles":null,"total":0}'
expect_bpc_fail "a null release_bundles fails closed" "failing closed"

bpc_case
JF_OUT='{"release_bundles":[{"release_bundle_version":"x"}],"total":"2"}'
expect_bpc_fail "a non-numeric total fails closed" "failing closed"

# --limit=255 is the API maximum and the listing is newest-first, so a truncated page
# answers "not found" for everything older than it.
bpc_case
JF_OUT='{"release_bundles":[{"release_bundle_version":"8.9.9.9-1"}],"total":300}'
expect_bpc_fail "a truncated listing fails closed" "listing truncated"

###############################################################################
# refresh-docker-image-version.yaml :: Verify STAGE carries this run's images
###############################################################################

VP="${WORK}/verify-stage.sh"
block "$WF_VERSION" "Verify STAGE carries this run's images" >"$VP"

VWS="${WORK}/vws"
mkdir -p "$VWS"
# DIGEST_MAP: "<ref-substring> <digest|absent>" rules, first match wins.
cat >"${VWS}/docker" <<'STUB'
#!/usr/bin/env bash
ref="${4:-}"
while read -r pat ans; do
    [[ -n "$pat" ]] || continue
    [[ "$ref" == *"$pat"* ]] || continue
    [[ "$ans" != "absent" ]] || exit 1
    printf '{"digest":"%s"}' "$ans"
    exit 0
done <<<"${DIGEST_MAP:-}"
printf '{"digest":"%s"}' "${DEFAULT_DIGEST:-sha256:aaa}"
STUB
chmod +x "${VWS}/docker"

run_vp() {
    (
        set +u
        export PATH="${VWS}:${PATH}"
        export REGISTRY=artifact.example STAGE_DOCKER_REPO=stage-repo
        export COMMUNITY_DIGEST=sha256:aaa ENTERPRISE_DIGEST=sha256:bbb FEDERAL_DIGEST=sha256:ccc
        export COMMUNITY_TAG=reg/aerospike-server:8.1.2.5-1_T
        export ENTERPRISE_TAG=reg/aerospike-server-enterprise:8.1.2.5-1_T
        export FEDERAL_TAG=reg/aerospike-server-federal:8.1.2.5-1_T
        export DIGEST_MAP="${DIGEST_MAP:-}" DEFAULT_DIGEST="${DEFAULT_DIGEST:-}"
        bash "$VP"
    ) >"${WORK}/vp.log" 2>&1
}

DIGEST_MAP="aerospike-server-enterprise sha256:bbb
aerospike-server-federal sha256:ccc
aerospike-server sha256:aaa"
if run_vp; then
    pass "STAGE carrying this run's three digests passes"
else
    fail "all-matching digests were rejected -- $(tail -3 "${WORK}/vp.log")"
fi

# The case the whole check exists for: bundle-precheck handed over a coordinate it only
# knew the name of, and what landed in STAGE is not what this run built.
DIGEST_MAP="aerospike-server-enterprise sha256:WRONG
aerospike-server-federal sha256:ccc
aerospike-server sha256:aaa"
if run_vp; then
    fail "a foreign digest in STAGE was accepted"
elif grep -q "does not carry this run's images" "${WORK}/vp.log"; then
    pass "a foreign digest stops the run before the handoff is posted"
else
    fail "a foreign digest failed without naming the reason -- $(tail -3 "${WORK}/vp.log")"
fi

DIGEST_MAP="aerospike-server-federal absent
aerospike-server-enterprise sha256:bbb
aerospike-server sha256:aaa"
if run_vp; then
    fail "a missing image in STAGE was accepted"
elif grep -q "<absent>" "${WORK}/vp.log"; then
    pass "an image missing from STAGE is reported as absent"
else
    fail "a missing image was not reported as absent -- $(tail -3 "${WORK}/vp.log")"
fi

# All three are reported, not just the first: an operator fixing this needs the whole
# picture, and the loop deliberately does not bail out.
DIGEST_MAP="aerospike-server-enterprise sha256:WRONG
aerospike-server-federal sha256:WRONG
aerospike-server sha256:aaa"
run_vp
n=$(grep -c "does not carry this run's images" "${WORK}/vp.log")
if [[ "$n" -eq 2 ]]; then
    pass "every mismatched edition is named, not just the first"
else
    fail "reported ${n} mismatches, expected 2"
fi

###############################################################################
# Static checks: things no extracted step can see
###############################################################################
#
# The scan reads com.aerospike.refresh.run and the refresh workflow writes it, but the
# scan is tested against a fabricated LABEL_MAP -- so producer and consumer can drift
# apart with every suite green, and if they do, every refresh reads as "not a refresh"
# and the reconciler never publishes anything. Same for the `publish` job's two gates:
# they are job-level `if:` expressions, which no step extraction reaches.

LBL="com.aerospike.refresh.run"
n_lbl=$(grep -c "\"${LBL}\"" "$WF_VERSION" || true)
if [[ "$n_lbl" -eq 3 ]]; then
    pass "all three editions stamp ${LBL}"
else
    fail "${n_lbl} of 3 labels-json lines carry ${LBL}; the scan would read those refreshes as 'not a refresh'"
fi
if grep -q "REFRESH_LABEL: ${LBL}" "$WF_PUBLISH"; then
    pass "the reconciler reads the same label the refresh writes"
else
    fail "REFRESH_LABEL in $(basename "$WF_PUBLISH") does not match ${LBL}"
fi

publish_if="$(
    python3 - "$WF_PUBLISH" <<'INNER'
import sys, yaml
d = yaml.safe_load(open(sys.argv[1]))
print(" ".join(str(d["jobs"]["publish"].get("if", "")).split()))
INNER
)"
for gate in "IS_RELEASE_REF == 'true'" "SKIP_ALL != 'true'"; do
    if [[ "$publish_if" == *"$gate"* ]]; then
        pass "the publish job is gated on ${gate}"
    else
        fail "the publish job's if: lost ${gate} -- an off-ref or disarmed run could write to PROD"
    fi
done

# The Hub tag set and the scan's parity loop must agree: Hub never carries w.x.y, and
# requiring it made every patch-line tip report "needs publishing" on every scan forever.
# shellcheck disable=SC2016  # a literal grep pattern, not an expansion
if grep -q 'for t in "\$v" "\${v%\.\*\.\*}" latest; do' "$WF_PUBLISH"; then
    pass "Hub parity skips w.x.y, which Docker Hub never carries"
else
    fail "the Hub parity loop no longer matches the Hub tag set (PROD minus w.x.y)"
fi

# An EXACT count, not a floor. A section that silently stops running -- a renamed
# helper, a block moved above its own definitions -- looks exactly like a passing suite,
# which is the failure this file exists to catch in the workflows. A floor misses it the
# moment one case vanishes and another is added, which is how a case landed above its own
# helper here and ran as `command not found` with the suite still green.
#
# Bump this when you add or remove a case; that one-line edit is the point.
EXPECT_ASSERTIONS=171
if [[ "$assertions" -ne "$EXPECT_ASSERTIONS" ]]; then
    echo "✗ ${assertions} assertions ran, expected exactly ${EXPECT_ASSERTIONS}."
    echo "  Fewer: a section did not execute (look for 'command not found' above)."
    echo "  More: you added cases -- update EXPECT_ASSERTIONS."
    exit 1
fi

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "All refresh workflow step tests passed (${assertions} assertions)."
