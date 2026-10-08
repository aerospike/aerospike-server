#!/usr/bin/env bash
#
# Tests for image_packages.sh. `docker` is stubbed on PATH and replays canned apt and
# dpkg-query output, so nothing here pulls an image or talks to an archive.
#
# What is under test is the one property both callers depend on: this probe never
# answers "nothing pending" or "unchanged" from output it could not actually read.
# Every plausibility check here exists because the failure it catches is silent --
# an offline apt-get update still exits 0 and still prints "0 upgraded".

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/image_packages.sh"
WORK="$(mktemp -d)"
STUB="${WORK}/bin"
mkdir -p "$STUB"
trap 'rm -rf "$WORK"' EXIT

failures=0
pass() { echo "✓ $1"; }
fail() {
    echo "✗ $1"
    failures=$((failures + 1))
}

# The stub reads its scripted reply from files the case writes: STUB_RC, STUB_OUT,
# STUB_ERR. It also records its argv so the platform/--network assertions can read it.
cat >"${STUB}/docker" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_ARGV_LOG}"
[[ ! -f "${STUB_OUT}" ]] || cat "${STUB_OUT}"
[[ ! -f "${STUB_ERR}" ]] || cat "${STUB_ERR}" >&2
exit "$(cat "${STUB_RC}" 2>/dev/null || echo 0)"
STUBEOF
chmod +x "${STUB}/docker"

# GitHub's ubuntu runners have timeout(1); macOS does not ship it. Stub it only when
# missing, so CI exercises the real one.
if ! command -v timeout >/dev/null 2>&1; then
    printf '#!/usr/bin/env bash\nshift\nexec "$@"\n' >"${STUB}/timeout"
    chmod +x "${STUB}/timeout"
fi

run() {
    local mode=$1 rc=$2 out=$3 err=${4:-}
    printf '%s' "$rc" >"${WORK}/rc"
    printf '%s' "$out" >"${WORK}/out"
    printf '%s' "$err" >"${WORK}/err"
    : >"${WORK}/argv"
    PATH="${STUB}:${PATH}" \
        STUB_RC="${WORK}/rc" STUB_OUT="${WORK}/out" STUB_ERR="${WORK}/err" \
        STUB_ARGV_LOG="${WORK}/argv" \
        bash "$SCRIPT" "$mode" test-registry/aerospike-server:8.1.2.5 2>"${WORK}/stderr"
}

expect_ok() {
    local name=$1 want=$2 got rc
    got=$(run "${@:3}")
    rc=$?
    if [[ "$rc" -ne 0 ]]; then
        fail "${name}: rc=${rc} -- $(cat "${WORK}/stderr")"
    elif [[ "$got" != "$want" ]]; then
        fail "${name}: got [${got//$'\n'/,}], want [${want//$'\n'/,}]"
    else
        pass "$name"
    fi
}

expect_fail() {
    local name=$1 want_err=$2 rc err
    run "${@:3}" >/dev/null
    rc=$?
    err=$(cat "${WORK}/stderr")
    if [[ "$rc" -eq 0 ]]; then
        fail "${name}: rc=0, expected a refusal"
    elif [[ "$err" != *"$want_err"* ]]; then
        fail "${name}: stderr did not mention '${want_err}': ${err}"
    else
        pass "$name"
    fi
}

# Both Inst shapes the simulation emits. The candidate version is the first field
# inside the parentheses in each.
APT_UPGRADES='Reading package lists...
Building dependency tree...
Inst libssl3t64 [3.0.13-0ubuntu3.15] (3.0.13-0ubuntu3.16 Ubuntu:24.04/noble-security [amd64])
Inst libxml2 (2.9.14+dfsg-1.3ubuntu3.5 Ubuntu:24.04/noble-updates [amd64])
Conf libssl3t64 (3.0.13-0ubuntu3.16 Ubuntu:24.04/noble-security [amd64])
2 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.'

expect_ok "pending reports name and CANDIDATE version, sorted" \
    $'libssl3t64 3.0.13-0ubuntu3.16\nlibxml2 2.9.14+dfsg-1.3ubuntu3.5' \
    pending 0 "$APT_UPGRADES"

# The soname-bump shape this pipeline exists for: libssl3 -> libssl3t64 is a NEW
# package, which `apt-get upgrade` holds back silently and only dist-upgrade reports.
expect_ok "a new dependency is reported, not just an upgrade" \
    "libssl3t64 3.0.13-0ubuntu3.16" \
    pending 0 'Inst libssl3t64 (3.0.13-0ubuntu3.16 Ubuntu:24.04/noble-security [amd64])
1 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.'

expect_ok "a current image reports an empty set" "" \
    pending 0 '0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.'

# The offline case: --error-on=any is what makes the container exit non-zero here.
# Without it apt-get update exits 0, the simulation reads empty lists and prints
# "0 upgraded", and this probe would report the image current having read nothing.
expect_fail "an apt that could not reach the archive is a failure, not 'current'" \
    "could not run apt inside" \
    pending 100 "" "E: Failed to fetch http://archive.ubuntu.com/... Could not connect"

# No summary line means this is not a simulation that can be read -- a container that
# exited early, an image with no archive configured. An empty Inst count taken off it
# would read as "current".
expect_fail "apt output with no summary line is refused" \
    "unrecognised apt output" \
    pending 0 'Reading package lists...
Building dependency tree...'

if grep -q -- "--error-on=any" "${WORK}/argv"; then
    pass "pending runs apt-get update with --error-on=any"
else
    fail "pending did not pass --error-on=any: $(cat "${WORK}/argv")"
fi

# dpkg-query output: a real base image carries hundreds of packages.
make_dpkg() {
    local n=$1 i out=""
    for ((i = 0; i < n; i++)); do out+="pkg${i} 1.0-${i}"$'\n'; done
    printf '%s' "$out"
}

got=$(run installed 0 "$(make_dpkg 40)")
rc=$?
if [[ "$rc" -eq 0 && "$(grep -c . <<<"$got")" -eq 40 ]]; then
    if [[ "$got" == "$(sort <<<"$got")" ]]; then
        pass "installed emits the full set, sorted"
    else
        fail "installed output is not sorted"
    fi
else
    fail "installed on a plausible image: rc=${rc} lines=$(grep -c . <<<"$got")"
fi

# An error page, a partial read, or an image whose dpkg database did not mount: an
# empty-ish set compared against another empty-ish set reads as "unchanged", which is
# the one answer the verify-refresh caller must never get by accident.
expect_fail "an implausibly short package list is refused" \
    "implausibly short" \
    installed 0 "$(make_dpkg 5)"

expect_fail "a non-package line in the output is refused" \
    "unexpected non-package line" \
    installed 0 "$(make_dpkg 30)Unable to find image 'x' locally"

expect_fail "a failed dpkg-query is refused" \
    "could not run dpkg-query inside" \
    installed 1 "" "Error response from daemon: manifest unknown"

# docker writes implicit-pull progress to stderr naming the image ref. Merged into
# stdout it would land in the "package set" a caller diffs, and two refs for one
# identical image would always compare as different.
got=$(run installed 0 "$(make_dpkg 30)" "Unable to find image 'test-registry/aerospike-server:8.1.2.5' locally")
rc=$?
if [[ "$rc" -eq 0 && "$got" != *aerospike-server* ]]; then
    pass "docker's pull progress on stderr stays out of the package set"
else
    fail "stderr leaked into the package set: ${got}"
fi

# installed must not reach the network: it reads a local database, and a probe that
# could pull mid-comparison would be comparing two different things.
if grep -q -- "--network none" "${WORK}/argv"; then
    pass "installed runs with --network none"
else
    fail "installed did not pass --network none: $(cat "${WORK}/argv")"
fi

expect_fail "an unknown mode is refused" "unknown mode" bogus 0 ""

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "All image_packages tests passed."
