#!/usr/bin/env bash
#
# Tests for image_exists.sh, with `docker` stubbed on PATH.
#
# The point of the script is that "absent" and "could not tell" are different answers,
# so every case here is one real registry message mapped to the exit code its caller
# must branch on. A 401, 429 or 5xx reading as "no published image" is what would let a
# registry blip promote an unverified refresh.

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/image_exists.sh"
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

cat >"${STUB}/docker" <<'STUBEOF'
#!/usr/bin/env bash
[[ ! -s "${STUB_OUT}" ]] || cat "${STUB_OUT}"
[[ ! -s "${STUB_ERR}" ]] || cat "${STUB_ERR}" >&2
exit "$(cat "${STUB_RC}")"
STUBEOF
chmod +x "${STUB}/docker"

# GitHub's ubuntu runners have timeout(1); macOS does not ship it. Stub it only when
# missing, so CI exercises the real one.
if ! command -v timeout >/dev/null 2>&1; then
    printf '#!/usr/bin/env bash\nshift\nexec "$@"\n' >"${STUB}/timeout"
    chmod +x "${STUB}/timeout"
fi

expect() {
    local name=$1 want_rc=$2 rc=$3 stderr=$4 stdout=${5:-}
    printf '%s' "$rc" >"${WORK}/rc"
    printf '%s' "$stdout" >"${WORK}/out"
    printf '%s' "$stderr" >"${WORK}/err"
    local got_rc
    PATH="${STUB}:${PATH}" STUB_RC="${WORK}/rc" STUB_OUT="${WORK}/out" STUB_ERR="${WORK}/err" \
        bash "$SCRIPT" registry.example/aerospike-server:8.1.2.5 >/dev/null 2>"${WORK}/stderr"
    got_rc=$?
    if [[ "$got_rc" -eq "$want_rc" ]]; then
        pass "$name"
    else
        fail "${name}: exit ${got_rc}, expected ${want_rc}"
    fi
}

expect "a present manifest exits 0" 0 0 "" '{"schemaVersion":2}'

# The two spellings a registry uses for a tag that is not there.
expect "'no such manifest' is absent (2)" 2 1 \
    "no such manifest: registry.example/aerospike-server:8.1.2.5"
expect "'manifest unknown' is absent (2)" 2 1 \
    "errors:
denied: requested access to the resource is denied
unauthorized: manifest unknown"

# Everything below used to land in the same branch as absence, and each one would have
# published all three editions without a comparison.
expect "a 401 is 'could not tell' (1)" 1 1 \
    "errors:
denied: requested access to the resource is denied
unauthorized: authentication required"
expect "a 429 is 'could not tell' (1)" 1 1 \
    "toomanyrequests: You have reached your pull rate limit."
expect "a 5xx is 'could not tell' (1)" 1 1 \
    "error parsing HTTP 503 response body: unexpected end of JSON input"
expect "a timeout is 'could not tell' (1)" 1 1 \
    "Get \"https://registry.example/v2/\": dial tcp: i/o timeout"
expect "a DNS failure is 'could not tell' (1)" 1 1 \
    "Get \"https://registry.example/v2/\": dial tcp: lookup registry.example: no such host"

# An error this script has never seen must fail toward the unsafe answer, not toward
# absence -- that is the whole asymmetry.
expect "an unrecognised error is 'could not tell' (1)" 1 1 \
    "something nobody has written a case for yet"

# timeout(1) exits 124 on expiry, with no output. That is not one of the absent
# spellings, so a stalled registry lands in "could not tell" rather than reading as a
# tag that is not there -- which would publish without a comparison.
expect "a timeout (rc 124) is 'could not tell' (1)" 1 124 ""

# The ref is mandatory: without it `docker manifest inspect` would be called with no
# argument and its usage error would read as "could not tell" for a ref nobody named.
if PATH="${STUB}:${PATH}" bash "$SCRIPT" >/dev/null 2>&1; then
    fail "a missing ref argument was accepted"
else
    pass "a missing ref argument fails"
fi

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "All image_exists tests passed."
