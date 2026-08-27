#!/usr/bin/env bash
# Drive every grading branch of report_client_test.bash from fixture logs.
#
# The point of extracting the reporter was that its interesting branches -- the
# ones that distinguish a non-obvious state -- only ever executed on a pushed
# six-leg matrix run, so most of them had never executed at all. Each case here
# asserts the exit status, the check-run conclusion, the GFM alert and the
# prose together, because the defect this suite exists to prevent is those four
# disagreeing with each other.
#
# curl is stubbed by a PATH shim that records its argv and payload, so the
# Checks API paths (create, update, missing curl, non-2xx) run offline.
#
# Usage: test_report_client_test.bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPORTER="${SCRIPT_DIR}/report_client_test.bash"
BASH_BIN="$BASH"

SANDBOX="$(mktemp -d "${TMPDIR:-/tmp}/report-client-test.XXXXXX")"
FIXTURES="${SANDBOX}/fixtures"
NOCURL="${SANDBOX}/nocurl"
mkdir -p "$FIXTURES" "$NOCURL"
trap 'rm -rf "$SANDBOX"' EXIT

failures=0
failures_mark=0
cases=0
case_name=""
work=""

# The 40 hex bytes every case publishes against. Hoisted out of run_case so an
# assertion can name the same value the reporter was handed.
DEFAULT_HEAD_SHA="0123456789abcdef0123456789abcdef01234567"

# A function name run inside the fresh sandbox after $work exists and before
# the reporter starts, for the cases that need something planted there. Cleared
# after each case, so it never leaks into the next one.
case_setup=""

# --- Fixtures -----------------------------------------------------------------------

# Write a test log with the client's real shape -- one status-glyph line per
# suite, then the run total -- plus the sentinel the test step writes once make
# returns. Args: <name> <suite> <passed> <total> [<suite> <passed> <total>...]
mkfixture() {
    local name="$1" path total_cases=0 total_passed=0
    shift
    path="${FIXTURES}/${name}.log"

    : >"$path"
    while [[ $# -gt 0 ]]; do
        local suite="$1" passed="$2" total="$3"
        shift 3
        if [[ "$passed" == "$total" ]]; then
            printf '[v] %s: %s/%s tests passed.\n' "$suite" "$passed" "$total" >>"$path"
        else
            printf '[x] %s: %s/%s tests passed.\n' "$suite" "$passed" "$total" >>"$path"
        fi
        total_cases=$((total_cases + 10#$total))
        total_passed=$((total_passed + 10#$passed))
    done
    printf '%s tests: %s passed, %s failed\n' \
        "$total_cases" "$total_passed" "$((total_cases - total_passed))" >>"$path"

    touch "${FIXTURES}/${name}.done"
}

mkfixture clean string 22 22 scan_basics 25 25
mkfixture one_failure string 21 22
mkfixture four_failures string 18 22
mkfixture five_failures string 17 22
mkfixture all_suites_pass string 22 22

# A log that reports nothing parseable: make died before the summary block.
printf 'make: *** [test] Error 1\n' >"${FIXTURES}/no_summary.log"
touch "${FIXTURES}/no_summary.done"

# The client changed its per-suite format. Must read as a parse failure, not as
# a clean run over zero suites.
printf '[v] string >> 22 of 22 tests OK\n' >"${FIXTURES}/moved_format.log"
touch "${FIXTURES}/moved_format.done"

# Killed mid-run: one counted failure, no run total, and deliberately no
# sentinel -- the step never got to write it.
printf '[x] string: 21/22 tests passed.\n' >"${FIXTURES}/truncated.log"

# The two parsers disagree, so one of the formats moved. 6 outranks 1.
printf '[x] string: 21/22 tests passed.\n9 tests: 3 passed, 6 failed\n' \
    >"${FIXTURES}/disagreeing.log"
touch "${FIXTURES}/disagreeing.done"

# Colour codes and CRLF: the old pattern required ']' immediately followed by a
# space and the line to end at the period, so either one dropped the suite.
printf '\033[31m[x]\033[0m string: 21/22 tests passed.\033[0m\r\n22 tests: 21 passed, 1 failed\r\n' \
    >"${FIXTURES}/ansi_crlf.log"
touch "${FIXTURES}/ansi_crlf.done"

# The old suite-name whitelist dropped any name carrying '-', '.' or a space.
printf '[x] list-ops.basic: 3/5 tests passed.\n5 tests: 3 passed, 2 failed\n' \
    >"${FIXTURES}/punctuated.log"
touch "${FIXTURES}/punctuated.done"

# A run total with no suite lines. Graded green as 'all 0 suites passed' until
# the parse-failure arm stopped requiring the total line to be missing too.
printf '99 tests: 99 passed, 0 failed\n' >"${FIXTURES}/total_only.log"
touch "${FIXTURES}/total_only.done"

# Suite lines and no run total, with the sentinel present: make RETURNED, so
# the killed-step arm cannot see this. A run that died after one of ~30 suites
# must not be filed as a tidy one-case regression.
printf '[x] string: 21/22 tests passed.\n' >"${FIXTURES}/no_total.log"
touch "${FIXTURES}/no_total.done"

# A bare CR inside a suite name -- a progress-bar redraw, not a line ending.
# The old trailing-only strip carried it into the workflow command, where the
# runner reads a lone CR as a line break.
printf '[x] a\rb: 1/2 tests passed.\n2 tests: 1 passed, 1 failed\n' \
    >"${FIXTURES}/cr_suite.log"
touch "${FIXTURES}/cr_suite.done"

# A non-zero run total with no suite lines: the loudest drift there is, and the
# one the `suites -gt 0` conjunct used to swallow.
printf '99 tests: 90 passed, 9 failed\n' >"${FIXTURES}/total_only_failed.log"
touch "${FIXTURES}/total_only_failed.done"

# Leading zeros in the client's own digit strings. bash arithmetic applies C
# integer-literal rules, so '09' is not a literal at all: it aborts the grader
# from a loop BODY, where errexit is NOT exempt, taking the check run, the step
# summary and the $GITHUB_OUTPUT write with it and leaving a bare 'value too
# great for base' in the step log as the only diagnostic.
printf '[x] string: 09/18 tests passed.\n18 tests: 09 passed, 09 failed\n' \
    >"${FIXTURES}/padded.log"
touch "${FIXTURES}/padded.done"

# The quieter half: 0100 IS a legal octal literal, so nothing is printed and the
# leg publishes 64 where the client said 100.
printf '[x] string: 0/0100 tests passed.\n0100 tests: 0 passed, 0100 failed\n' \
    >"${FIXTURES}/padded_octal.log"
touch "${FIXTURES}/padded_octal.done"

# Padding on the run-total line only. Those comparisons sit in CONDITION
# position, where errexit IS exempt, so this failed silently: '-ge 0' read false
# and skipped the suite/total cross-check that exists to catch a format drift,
# '-lt 0' read false and skipped the 'no total line' arm, and the leg graded on
# the suite count of 1 while the run total said 8.
printf '[x] string: 21/22 tests passed.\n22 tests: 14 passed, 08 failed\n' \
    >"${FIXTURES}/padded_total.log"
touch "${FIXTURES}/padded_total.done"

# A pid that cannot be running, for the 'asd died' arm.
printf '2147480000\n' >"${FIXTURES}/dead.pid"

# A pid that exists but is not asd. kill -0 vouches for it; the identity check
# does not.
printf '%s\n' "$$" >"${FIXTURES}/notasd.pid"

# A PATH holding the coreutils the reporter uses and nothing else -- notably no
# curl, so the 'curl not found' branch is reachable without also taking away
# the tools the script needs to get that far.
for tool in sed tr tail cat readlink; do
    ln -sf "$(command -v "$tool")" "${NOCURL}/${tool}"
done

# --- Harness ------------------------------------------------------------------------

# A curl that never leaves the machine. Records argv (so the request URL and
# every --data-urlencode literal are assertable), keeps any --data @file body
# and any --config, and answers with the status the case asked for. The
# check-run lookup (-G) is answered separately from the write, so a case can
# have an existing check run without also pinning the write's status.
make_curl_shim() {
    mkdir -p "${work}/bin"
    cat >"${work}/bin/curl" <<'SHIM'
#!/usr/bin/env bash
set -uo pipefail

out=""
payload=""
authcfg=""
lookup=0
prev=""
for arg in "$@"; do
    [[ "$prev" == "-o" ]] && out="$arg"
    [[ "$arg" == "-G" ]] && lookup=1
    [[ "$prev" == "--data" && "$arg" == @* ]] && payload="${arg#@}"
    [[ "$prev" == "-K" ]] && authcfg="$arg"
    prev="$arg"
done

printf '%s\n' "$*" >>"${SHIM_LOG}"

# The config as curl received it, from stdin or from a path. Recording only
# that -K appeared would let an EMPTY config pass an argv assertion, which is
# the one-sided half of 'the token is not in argv'.
if [[ -n "$authcfg" ]]; then
    if [[ "$authcfg" == "-" ]]; then
        cat >"${SHIM_AUTHCFG}"
    else
        cp "$authcfg" "${SHIM_AUTHCFG}"
    fi
fi

if [[ "$lookup" == 1 ]]; then
    [[ -n "$out" ]] && printf '%s' "${SHIM_LOOKUP_BODY}" >"$out"
    printf '%s' "${SHIM_LOOKUP_CODE}"
    exit 0
fi

[[ -n "$payload" ]] && cp "$payload" "${SHIM_PAYLOAD}"
[[ -n "$out" ]] && printf '%s' '{}' >"$out"
printf '%s' "${SHIM_WRITE_CODE}"
exit 0
SHIM
    chmod +x "${work}/bin/curl"
}

# --- Sandbox hooks (set case_setup=<name> before a run_case) -------------------------

# A curl config sitting at the path the reporter used to write to. -K takes a
# whole curl config and not just a header, so honouring one that is already
# there hands an attacker `url`, `output` and `write-out` on both calls.
plant_auth_conf() {
    printf 'url = "http://127.0.0.1:9/"\noutput = "/dev/null"\n' \
        >"${work}/tmp/checks-auth.conf"
}

# readlink that answers nothing, the way /proc/<pid>/exe reads back empty under
# qemu. asd_alive()'s identity check is best-effort and returns ALIVE on an
# answer it could not get, so this is what leaves the process-state check as
# the only thing deciding.
break_readlink() {
    printf '#!/usr/bin/env bash\nexit 1\n' >"${work}/bin/readlink"
    chmod +x "${work}/bin/readlink"
}

# Run one case in a fresh sandbox.
#
#   run_case <description> <fixture|-> [KEY=VALUE...]
#
# <fixture> names a mkfixture log to grade; '-' keeps the clean one. The
# KEY=VALUE arguments override the defaults below. Records rc, stdout+stderr,
# the job summary, the published payload and the curl argv log.
run_case() {
    local fixture
    case_name="$1"
    fixture="$2"
    shift 2
    [[ "$fixture" == "-" ]] && fixture="clean"

    failures_mark="$failures"
    work="$(mktemp -d "${SANDBOX}/case.XXXXXX")"
    mkdir -p "${work}/tmp" "${work}/cwd"
    make_curl_shim
    : >"${work}/summary.md"
    : >"${work}/curl.log"
    : >"${work}/payload.json"
    : >"${work}/authcfg"
    : >"${work}/github_output"

    if [[ -n "$case_setup" ]]; then
        "$case_setup"
        case_setup=""
    fi

    (
        # Deliberately not the workspace. The reporter's step declares no
        # working-directory:, so in CI its cwd already IS $GITHUB_WORKSPACE and
        # a missing in_workspace() join resolves anyway -- which is why deleting
        # the join was invisible. Running from somewhere else is what makes the
        # join load-bearing for the relative cases below.
        cd "${work}/cwd" || exit 99
        export PATH="${work}/bin:${PATH}"
        export GITHUB_WORKSPACE="$work"
        export RUNNER_TEMP="${work}/tmp"
        export GITHUB_STEP_SUMMARY="${work}/summary.md"
        export GITHUB_OUTPUT="${work}/github_output"
        export GITHUB_API_URL="https://api.github.invalid"
        export GITHUB_REPOSITORY="citrusleaf/aerospike-server"

        export SHIM_LOG="${work}/curl.log"
        export SHIM_PAYLOAD="${work}/payload.json"
        export SHIM_AUTHCFG="${work}/authcfg"
        export SHIM_LOOKUP_BODY='{"total_count":0,"check_runs":[]}'
        export SHIM_LOOKUP_CODE=200
        export SHIM_WRITE_CODE=201

        export REF="7.5.0"
        export DISTRO="el9"
        export SHA="deadbee"
        export BUILD_OUTCOME="success"
        export TEST_OUTCOME="success"
        export HEAD_SHA="$DEFAULT_HEAD_SHA"
        export CHECKS_TOKEN="x"
        export RUN_URL="https://github.invalid/run/1"
        export TEST_LOG="${FIXTURES}/${fixture}.log"
        export TEST_SENTINEL="${FIXTURES}/${fixture}.done"

        for kv in "$@"; do
            export "${kv?}"
        done

        "$BASH_BIN" "$REPORTER"
    ) >"${work}/out.txt" 2>&1 && printf 0 >"${work}/rc" || printf '%s' "$?" >"${work}/rc"
}

# --- Assertions ---------------------------------------------------------------------

fail() {
    echo "  ✗ ${case_name}: $1"
    failures=$((failures + 1))
}

expect_rc() {
    local got
    got="$(cat "${work}/rc")"
    [[ "$got" == "$1" ]] || fail "exit status ${got}, expected $1"
}

expect_conclusion() {
    grep -q "\"conclusion\": \"$1\"" "${work}/payload.json" ||
        fail "conclusion is not '$1' — payload: $(tr -d '\n' <"${work}/payload.json")"
}

expect_no_publish() {
    [[ ! -s "${work}/payload.json" ]] || fail "published a check run when none was expected"
}

expect_alert() {
    grep -q "^> \[!$1\]" "${work}/summary.md" ||
        fail "summary alert is not $1 — got: $(head -n 1 "${work}/summary.md")"
}

expect_out() {
    grep -qF -- "$1" "${work}/out.txt" || fail "stdout is missing: $1"
}

expect_summary() {
    grep -qF -- "$1" "${work}/summary.md" || fail "summary is missing: $1"
}

expect_not_summary() {
    if grep -qF -- "$1" "${work}/summary.md"; then
        fail "summary should not contain: $1"
    fi
}

expect_payload() {
    grep -qF -- "$1" "${work}/payload.json" || fail "payload is missing: $1"
}

expect_curl_method() {
    grep -q -- "-X $1 " "${work}/curl.log" ||
        fail "no $1 was issued — curl calls: $(tr '\n' '|' <"${work}/curl.log")"
}

expect_curl() {
    grep -qF -- "$1" "${work}/curl.log" ||
        fail "no curl call mentioned '$1' — curl calls: $(tr '\n' '|' <"${work}/curl.log")"
}

# The payload must stay JSON whatever the ref contained. python3 is the one
# JSON parser guaranteed on both the runner and a developer's machine.
expect_valid_json() {
    python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "${work}/payload.json" 2>/dev/null ||
        fail "payload is not valid JSON: $(tr -d '\n' <"${work}/payload.json")"
}

# Validity is not enough: an injected quote can produce a payload that parses
# and still says something other than what the reporter composed. Assert the
# PARSED value, and that no key the reporter never writes appeared.
expect_json_field() {
    local got
    got="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d.get(sys.argv[2], d.get("output", {}).get(sys.argv[2], "<absent>")))' \
        "${work}/payload.json" "$1" 2>/dev/null)" || got="<unparseable>"
    [[ "$got" == "$2" ]] || fail "payload .$1 is '${got}', expected '$2'"
}

expect_json_no_key() {
    python3 -c 'import json,sys; sys.exit(1 if sys.argv[2] in json.load(open(sys.argv[1])) else 0)' \
        "${work}/payload.json" "$1" 2>/dev/null ||
        fail "payload gained a '$1' key it never writes"
}

# A workflow command only counts if it starts a line, so this is the assertion
# an injected newline or CR has to survive.
expect_no_workflow_command() {
    if grep -qE "^::${1}" "${work}/out.txt"; then
        fail "stdout has a line beginning '::${1}': $(grep -nE "^::${1}" "${work}/out.txt" | head -n 1)"
    fi
}

expect_no_curl() {
    if grep -qF -- "$1" "${work}/curl.log"; then
        fail "curl's argv leaked '$1'"
    fi
}

# The credential as curl actually received it. Asserting only that the token is
# absent from argv is one-sided: a config carrying no Authorization header at
# all satisfies it, and then the check run is simply never published while the
# grade still exits normally.
expect_authcfg() {
    if [[ ! -s "${work}/authcfg" ]]; then
        fail "curl was handed no --config, so nothing carried the credential"
        return
    fi
    grep -qF -- "$1" "${work}/authcfg" ||
        fail "the curl config is missing '$1' — got: $(tr -d '\n' <"${work}/authcfg")"
}

# RUNNER_TEMP outlives every call in the step and its contents are readable by
# the uid the client's make ran as, so anything the reporter leaves there is a
# no-write read for the rest of the step.
expect_no_runner_temp_file() {
    [[ ! -e "${work}/tmp/$1" ]] || fail "left '${1}' behind in RUNNER_TEMP"
}

# The grade as a step output. The upload step gates on this and NOT on the
# reporter's exit status, which collapses success and neutral onto 0 by design
# -- so every leg that failed before the client build exits 0 while grading
# grey, and reading the outcome skipped the upload across exactly the region
# where asd.log is the only evidence. Asserted for each conclusion, because a
# missing line reads as 'not success' and would upload rather than fail.
expect_step_output_conclusion() {
    local got
    got="$({ grep -m1 '^conclusion=' "${work}/github_output" || true; } | sed 's/^conclusion=//')"
    [[ "$got" == "$1" ]] ||
        fail "GITHUB_OUTPUT conclusion is '${got:-<absent>}', expected '$1'"
}

# Only tick a case that had no failed assertion, so a ✗ is not followed by a
# ✓ for the same case. Also the one place a case is COUNTED, so that a green
# exit can mean a known set of cases ran rather than an unknown subset -- see
# EXPECTED_CASES at the foot of this file.
ok() {
    if [[ "$failures" -eq "$failures_mark" ]]; then
        cases=$((cases + 1))
        echo "  ✓ ${case_name}"
    fi
}

# An unmet host precondition is CI's problem and the developer's information.
# Six cases here are gated on /proc existing, on `readlink -f /proc/<pid>/exe`
# answering, and on TCP 3000 being free; all three are genuinely absent on some
# developer machines, and all three are guaranteed on the reviewdog runner. A
# skip used to print a line and leave the exit status at 0, so on a host that
# could not run them a mutation those very cases exist to kill was invisible,
# announced only in a line no green run is read for. In-tree precedent for the
# polarity: the zombie preconditions this suite controls already `fail`.
skip_or_fail() {
    case_name="$1"
    if [[ -n "${CI:-}${GITHUB_ACTIONS:-}" ]]; then
        failures_mark="$failures"
        fail "$2, and this host is CI, where every case must run"
    else
        echo "  - skipped '$1': $2"
    fi
}

# --- Cases: grading -----------------------------------------------------------------

echo "Grading:"

run_case "a clean run is green and passes the job" clean TEST_OUTCOME=success
expect_rc 0
expect_conclusion success
expect_step_output_conclusion success
expect_alert TIP
expect_summary "all 2 suites passed"
ok

# A small count is red to the check run and yellow to the job summary, and that
# is not an inconsistency -- it is the only place the two surfaces can differ.
# `action_required` was measured on this PR's own check runs: `gh pr checks`
# prints it as `fail` and the commit rollup is FAILURE, so it bought no colour
# anywhere, carried no actions[] for the thing GitHub uses it for, and would
# block a merge exactly like `failure` the moment these checks are required.
run_case "1 failed case is red, and yellow in the summary" one_failure TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_step_output_conclusion failure
expect_alert WARNING
expect_summary "1 failed test case"
expect_summary "string 21/22"
ok

run_case "4 failed cases are red, and yellow in the summary" four_failures TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_alert WARNING
expect_summary "4 failed test cases"
ok

# The tier boundary: same conclusion as the case above, different alert. Both
# halves are asserted on both sides of it, or nothing distinguishes them.
run_case "5 failed cases are red in the summary too" five_failures TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_step_output_conclusion failure
expect_alert CAUTION
expect_summary "5 failed test cases"
ok

# --- Cases: counts that are not the integers they look like -------------------------

echo "Counts that are not the integers they look like:"

run_case "a zero-padded count is decimal, not octal" padded TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_summary "9 failed test cases"
ok

run_case "a count that is a legal octal is still decimal" padded_octal TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_summary "100 failed test cases"
ok

# The silent one: both comparisons on total_failures are in condition position,
# so errexit did not stop this -- the cross-check and the 'no total line' arm
# were both skipped and the leg graded on the suite count of 1.
run_case "a zero-padded run total still cross-checks" padded_total TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_out "disagree"
expect_summary "8 failed test cases"
ok

# --- Cases: states where the count is not trustworthy -------------------------------

echo "States where the count is not trustworthy:"

# The defect this suite was written for. The earlier shape took the colour from
# the count and the prose and exit status from make's status, so this published
# a red check run whose body read 'every test passed', and left the job green.
run_case "counted failures with a passing exit status" five_failures TEST_OUTCOME=success
expect_rc 1
expect_conclusion failure
expect_alert CAUTION
expect_not_summary "all tests passed"
expect_payload "5 failed test cases"
ok

run_case "a failing exit status with no counted failures" all_suites_pass TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_alert CAUTION
expect_payload "every counted suite passed"
ok

# 'No lines matched' used to be indistinguishable from 'everything passed'.
run_case "an unparseable log is red, not green" no_summary TEST_OUTCOME=success
expect_rc 1
expect_conclusion failure
expect_payload "could not parse"
expect_not_summary "all tests passed"
ok

run_case "a changed summary format is red" moved_format TEST_OUTCOME=success
expect_rc 1
expect_conclusion failure
expect_payload "could not parse"
ok

# The parse-failure arm used to require the run total to be missing too, so a
# per-suite format drift that left the total intact graded GREEN, under the
# words 'all 0 suites passed'. Every leg would have gone permanently green with
# no warning that anything had stopped being counted.
run_case "a run total with no suite lines is red, not green" total_only TEST_OUTCOME=success
expect_rc 1
expect_conclusion failure
expect_payload "could not parse"
expect_not_summary "all 0 suites passed"
ok

# make RETURNED here, so the sentinel is present and the killed-step arm cannot
# see this by construction. The absent total line is the only signal that the
# count is a lower bound rather than the whole story.
run_case "suite lines with no run total are a lower bound, not a result" no_total TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_payload "did not reach its total line"
expect_not_summary "1 failed test case"
ok

# A killed step: sentinel absent, log truncated after one counted failure. Must
# not be filed as a tidy '1 failed test case' yellow.
run_case "a killed test step is red, not a small count" truncated TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_alert CAUTION
expect_summary "killed"
ok

# A server that died mid-suite is not a client disagreement, and must not be
# reported as one.
run_case "asd dying is not a client disagreement" one_failure \
    TEST_OUTCOME=failure "ASD_PID_FILE=${FIXTURES}/dead.pid"
expect_rc 1
expect_conclusion failure
expect_payload "server died"
expect_not_summary "investigate the server change"
ok

run_case "disagreeing counts grade on the larger" disagreeing TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure # 6 from the run total, not the 1 the suite line reports
expect_out "disagree"
ok

# A run total reporting failures that not one suite line accounted for. The
# `suites -gt 0` conjunct made this the one drift that never warned.
run_case "a non-zero total with no suite lines reports the drift" total_only_failed \
    TEST_OUTCOME=failure
expect_out "disagree"
ok

# kill -0 succeeds on a zombie, and on the three el9 legs asd is guaranteed to
# become one -- the job container's PID 1 is `tail -f /dev/null`, which never
# wait()s. That made 'the server died' unreachable on half the matrix.
if [[ -d /proc ]]; then
    python3 - "${FIXTURES}/zombie.pid" <<'ZOMBIE' &
import os, sys, time

pid = os.fork()
if pid == 0:
    os._exit(0)
with open(sys.argv[1], "w") as fh:
    fh.write("%d\n" % pid)
time.sleep(60)
ZOMBIE
    zombie_parent=$!

    zombie_state=""
    for _ in $(seq 1 100); do
        if [[ -s "${FIXTURES}/zombie.pid" ]]; then
            zombie_state="$(sed -nE 's/^.*\) ([A-Za-z]).*/\1/p' \
                "/proc/$(cat "${FIXTURES}/zombie.pid")/stat" 2>/dev/null || true)"
            [[ "$zombie_state" == "Z" ]] && break
        fi
        sleep 0.1
    done

    case_name="a zombie asd is dead, not alive"
    failures_mark="$failures"

    if [[ "$zombie_state" != "Z" ]]; then
        fail "could not produce a zombie to test against (state '${zombie_state:-none}')"
    else
        run_case "a zombie asd is dead, not alive" one_failure \
            TEST_OUTCOME=failure "ASD_PID_FILE=${FIXTURES}/zombie.pid"
        expect_rc 1
        expect_conclusion failure
        expect_payload "server died"
        ok

        # The case above does not actually exercise the state check. For a
        # zombie, `readlink -f /proc/<z>/exe` canonicalises the dangling link
        # rather than failing, so it prints the literal '/proc/<z>/exe' -- which
        # is non-empty and does not end in /asd, and the IDENTITY line returns
        # 'dead' before the state line's answer can matter. Deleting
        # `[[ "$state" != "Z" ]] || return 1` outright leaves it green.
        #
        # Take the identity half away and the state check is the only thing
        # left. This is also the shape that occurs for real: the residual risk
        # the state check covers is precisely the best-effort path where
        # readlink cannot answer, which without it reads a zombie as alive.
        case_setup=break_readlink
        run_case "a zombie is dead even when /proc/<pid>/exe cannot be read" one_failure \
            TEST_OUTCOME=failure "ASD_PID_FILE=${FIXTURES}/zombie.pid"
        expect_rc 1
        expect_conclusion failure
        expect_payload "server died"
        ok
    fi

    kill "$zombie_parent" 2>/dev/null || true
    wait "$zombie_parent" 2>/dev/null || true
else
    skip_or_fail "a zombie asd is dead, not alive" "no /proc on this host"
    skip_or_fail "a zombie is dead even when /proc/<pid>/exe cannot be read" \
        "no /proc on this host"
fi

# A live pid that is not asd. The recorded pid is up to 25 minutes old by the
# time it is read, so it can have been recycled by something else entirely.
#
# The identity half of asd_alive() is deliberately best-effort -- an answer we
# could not get must not be read as a death -- so this case is only meaningful
# where the host will actually answer. It does on the runners; it does not
# under qemu emulation, where /proc/<pid>/exe reads back empty.
if [[ -n "$(readlink -f "/proc/$$/exe" 2>/dev/null || true)" ]]; then
    run_case "a recycled pid does not vouch for the server" one_failure \
        TEST_OUTCOME=failure "ASD_PID_FILE=${FIXTURES}/notasd.pid"
    expect_rc 1
    expect_conclusion failure
    expect_payload "server died"
    ok
else
    skip_or_fail "a recycled pid does not vouch for the server" \
        "this host does not resolve /proc/<pid>/exe"
fi

# The other side of the same predicate. Every case above asks asd_alive() only
# to say 'dead', so nothing constrains its true branch -- mutating the `*/asd`
# glob to anything unmatchable leaves the suite green while turning EVERY
# healthy leg red under 'the server died during the client tests'. By the time
# the reporter runs on a real leg the pid file always exists, so this line is
# the last thing deciding the grade on every leg that got that far.
if [[ -d /proc ]] && [[ -n "$(readlink -f "/proc/$$/exe" 2>/dev/null || true)" ]]; then
    cp "$(command -v sleep)" "${FIXTURES}/asd"
    "${FIXTURES}/asd" 300 &
    live_asd=$!
    printf '%s\n' "$live_asd" >"${FIXTURES}/liveasd.pid"

    if [[ "$(readlink -f "/proc/${live_asd}/exe" 2>/dev/null || true)" == */asd ]]; then
        run_case "a live asd vouches for the server" clean \
            TEST_OUTCOME=success "ASD_PID_FILE=${FIXTURES}/liveasd.pid"
        expect_rc 0
        expect_conclusion success
        expect_json_field title "all tests passed"
        ok
    else
        skip_or_fail "a live asd vouches for the server" \
            "/proc/<pid>/exe does not resolve to the copy"
    fi

    kill "$live_asd" 2>/dev/null || true
    wait "$live_asd" 2>/dev/null || true
else
    skip_or_fail "a live asd vouches for the server" \
        "this host does not resolve /proc/<pid>/exe"
fi

# --- Cases: log robustness ----------------------------------------------------------

echo "Log robustness:"

run_case "ANSI colour and CRLF still parse" ansi_crlf TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_alert WARNING
expect_summary "1 failed test case"
ok

run_case "punctuated suite names still parse" punctuated TEST_OUTCOME=failure
expect_rc 1
expect_summary "list-ops.basic 3/5"
ok

# A CR anywhere in the line, not just at its end: the old strip was $-anchored,
# so a mid-line CR rode into the suite name and thence into a workflow command,
# where the runner reads a lone CR as a line break.
run_case "a mid-line CR is stripped from the suite name" cr_suite TEST_OUTCOME=failure
expect_rc 1
expect_summary "ab 1/2"
ok

# --- Cases: legs that never tested --------------------------------------------------

echo "Legs that never tested:"

run_case "a client that will not build is grey and passes" - BUILD_OUTCOME=failure
expect_rc 0
expect_conclusion neutral
expect_step_output_conclusion neutral
expect_alert NOTE
expect_payload "did not build"
ok

run_case "a leg that never started is grey and passes" - BUILD_OUTCOME=
expect_rc 0
expect_conclusion neutral
expect_alert NOTE
expect_summary "not-run"
ok

# --- Cases: ref classification ------------------------------------------------------

echo "Ref classification:"

classify_case() {
    run_case "ref '$1' reads as $2" one_failure "REF=$1" TEST_OUTCOME=failure
    expect_summary "$2"
    ok
}

classify_case "7.4.0" "shipped client"
classify_case "v7.6.0" "shipped client"
classify_case "7.6.0-rc1" "pre-release client"
classify_case "stage" "pre-release client"

# --- Cases: check-run transport -----------------------------------------------------

echo "Check-run transport:"

# A quote in an operator-supplied ref used to splice keys into the payload.
#
# expect_valid_json alone did not test anything here: the fixture was chosen so
# that the raw substitution produces WELL-FORMED JSON -- name truncates and a
# top-level "x" appears, and it parses. Deleting json_safe outright left this
# case green. Assert the parsed value and the absence of the spliced key.
run_case "a quote in the ref cannot break the payload" - 'REF=7.5.0" , "x": "y'
expect_valid_json
expect_json_field name 'C client 7.5.0 , x: y (el9)'
expect_json_no_key x
ok

# A newline in the ref reaches the ::error:: line main() emits, where a second
# line beginning '::' is a workflow command of the ref's choosing --
# ::stop-commands:: among them, which would silence every annotation after it.
run_case "a newline in the ref cannot start a workflow command" - \
    "$(printf 'REF=7.5.0\n::stop-commands::x')" TEST_OUTCOME=failure
expect_no_workflow_command "stop-commands"
expect_valid_json
ok

# SHA is not the 40 hex bytes it looks like. It is the client-build step's
# output, and that step runs the client's own make AFTER writing it, with
# GITHUB_OUTPUT in make's environment and step outputs last-write-wins.
run_case "a newline in the resolved SHA cannot start a workflow command" one_failure \
    "$(printf 'SHA=deadbee\n::stop-commands::x')" TEST_OUTCOME=failure
expect_no_workflow_command "stop-commands"
ok

# The token shares a workspace and a uid with the client's make, and
# /proc/<pid>/cmdline is mode 0444 -- readable by any uid on the box while the
# call is in flight. Both halves are asserted, because the negative one is
# satisfied on its own by a config carrying no Authorization header at all:
# publish_check then takes its non-2xx branch, warns, returns 0, and the check
# run silently never appears while the grade still exits normally.
run_case "the checks token is delivered, and not through argv" - CHECKS_TOKEN=tok_must_not_appear
expect_rc 0
expect_no_curl "tok_must_not_appear"
expect_authcfg "Authorization: Bearer tok_must_not_appear"
# On stdin rather than through a file: a config in RUNNER_TEMP advertises its
# own path in argv and outlives the call, which is the same no-write read one
# layer over -- and a longer window than the argv it replaced.
expect_curl "-K -"
expect_no_runner_temp_file "checks-auth.conf"
ok

# Pins the property rather than the mechanism: whatever is at that path, the
# credential still has to reach curl and the check run still has to land.
case_setup=plant_auth_conf
run_case "a config planted in RUNNER_TEMP is not honoured" -
expect_rc 0
expect_conclusion success
expect_authcfg "Authorization: Bearer"
ok

# Where the request went and which commit it hangs off -- neither is visible in
# the payload assertions, and both are silent when wrong. head_sha is a
# deliberate choice (github.event.pull_request.head.sha, not github.sha, or the
# check run lands on a merge commit no one is looking at); a regression there
# is a 422 that publish_check swallows into a ::warning::.
run_case "the check run is published against the PR head" -
expect_json_field head_sha "$DEFAULT_HEAD_SHA"
expect_json_field status completed
expect_json_field details_url "https://github.invalid/run/1"
expect_curl "/commits/${DEFAULT_HEAD_SHA}/check-runs"
# The list endpoint returns EVERY check run on the commit, and
# existing_check_id scrapes the first "id" in the array -- so without the
# server-side filter the reporter PATCHes whatever happens to be first
# (clang-format, shellcheck) and overwrites it with this leg's grade.
expect_curl "check_name=C client 7.5.0 (el9)"
ok

# 'Re-run failed jobs' reuses HEAD_SHA, so an existing check run of this name
# must be updated rather than joined by a second one with another conclusion.
run_case "a re-run updates its check run instead of adding one" - \
    'SHIM_LOOKUP_BODY={"total_count":1,"check_runs":[{"id":4242,"name":"C client 7.5.0 (el9)"}]}' \
    SHIM_WRITE_CODE=200
expect_curl_method PATCH
expect_curl "check-runs/4242"
ok

run_case "a first run creates its check run" -
expect_curl_method POST
ok

# Reporting never decides the leg's fate: a read-only fork token costs the
# colour, not the run.
run_case "a rejected publish warns and keeps the grade" - SHIM_WRITE_CODE=403
expect_rc 0
expect_out "could not publish"
ok

run_case "a missing curl warns and keeps the grade" - "PATH=${NOCURL}"
expect_rc 0
expect_out "curl not found"
expect_no_publish
ok

# --- Cases: the sentinel contract ---------------------------------------------------
#
# The reporter treats a missing sentinel as "the test step was killed", so the
# step has to write one whenever make actually returned. That depends on a
# detail of how GitHub invokes the step, not on anything in this directory:
# `shell: bash` runs as `bash --noprofile --norc -e -o pipefail {0}`, so errexit
# is already on and `set -uo pipefail` does NOT clear it. Without an explicit
# `set +e`, a failing suite ends the step at the pipeline and the sentinel is
# never written -- which the reporter then grades as an unbounded failure
# instead of the 1 counted case it was. That shipped once; this pins it.
#
# The step's own script is extracted from the workflow and run for real, so
# this cannot drift from what CI executes.

echo "The test step's sentinel contract:"

WORKFLOW="${SCRIPT_DIR}/../workflows/build-and-test.yaml"

# Pull one step's `run: |` block out of the workflow so it can be executed for
# real rather than paraphrased. Body lines are indented 10 spaces; the block
# ends at the first non-blank line indented less than that. One extraction,
# shared by every step test, so the indentation assumption has a single home.
extract_run_block() {
    awk -v want="^      - name: $1\$" '
        $0 ~ want                      { in_step = 1; next }
        in_step && /^        run: \|$/ { in_run = 1; next }
        in_run {
            if ($0 != "" && $0 !~ /^          /) { exit }
            print substr($0, 11)
        }
    ' "$WORKFLOW"
}

# Scoped to the client-test job rather than grepped file-wide: an env: block
# that drifted onto the wrong job would still answer a file-wide grep, and the
# steps that read these names would then die under set -u with the suite green
# -- which is the same class of defect these functions exist to catch.
CLIENT_TEST_JOB="$(awk '
    /^  client-test:$/ { in_job = 1; next }
    in_job && /^  [a-z]/ && !/^   / { exit }
    in_job { print }
' "$WORKFLOW")"

# The job env: block's value for a name, verbatim -- not its basename. The
# prefix is the half that matters: an absolute `${{ github.workspace }}` here
# is interpolated on the HOST, and the three el9 legs run in a container that
# bind-mounts the workspace somewhere else, so a host-rooted path names a
# directory that does not exist there and every one of those legs dies at
# 'Start Aerospike server' on a redirect it cannot open. A basename comparison
# stays green straight through that, which is how it shipped.
wf_value() {
    printf '%s\n' "$CLIENT_TEST_JOB" |
        { grep -m1 -E "^      $1: " || true; } | sed -E "s/^      $1: //"
}

# A STEP-level env: value, verbatim. wf_value reads the job block, whose entries
# sit at six spaces; a step's are four deeper.
wf_step_value() {
    printf '%s\n' "$CLIENT_TEST_JOB" |
        { grep -m1 -E "^          $1: " || true; } | sed -E "s/^          $1: //"
}

# The reporter repeats each name as a `${NAME:-<default>}` fallback for a local
# run, joined with the workspace by in_workspace().
rp_default() {
    { grep -m1 -E "^$1=" "$REPORTER" || true; } | sed -nE 's/.*:-([^}]*)\}.*/\1/p'
}

# Uses of $NAME in the job's shell that are NOT joined with $GITHUB_WORKSPACE
# -- the runner sets that one container-side, so it is the only root either
# half can agree on. Comments and the env: definition itself are out of scope;
# `${{ env.NAME }}` is not a shell reference and upload-artifact resolves a
# relative path against the workspace on its own.
unjoined_uses() {
    printf '%s\n' "$CLIENT_TEST_JOB" |
        grep -v '^ *#' |
        grep -v "^      $1: " |
        sed "s|\$GITHUB_WORKSPACE/\$$1||g" |
        { grep -cE '[$]\{?'"$1"'\}?' || true; }
}

# The workflow names these four files; the reporter repeats three of them as
# ${:-} fallbacks for a local run. Nothing in bash binds the two copies, and
# the suite is otherwise blind to a divergence -- every run_case exports its
# own TEST_LOG and TEST_SENTINEL, and ASD_PID_FILE's default points at a file
# the sandbox does not have, which takes asd_alive()'s early return. So a
# renamed asd.pid makes "the server died" unreachable forever, silently, with
# this file still green.
case_name="the workflow and the reporter agree on the file names"
failures_mark="$failures"

for _name in TEST_LOG TEST_SENTINEL ASD_PID_FILE; do
    _wf="$(wf_value "$_name")"
    _rp="$(rp_default "$_name")"
    if [[ -z "$_wf" ]]; then
        fail "${_name} is not defined in the client-test job's env: block"
    elif [[ "$_wf" != "$_rp" ]]; then
        fail "${_name}: build-and-test.yaml says '${_wf}', report_client_test.bash defaults to '${_rp}'"
    fi
done
ok

# The other half of the same contract, and the half a basename comparison
# cannot see: the names must be workspace-RELATIVE, and every shell use must
# join them with $GITHUB_WORKSPACE. Both sides have to move together or the
# steps and the reporter disagree about the root.
case_name="the file names are workspace-relative and joined at every use"
failures_mark="$failures"

for _name in ASD_LOG ASD_PID_FILE TEST_LOG TEST_SENTINEL; do
    _wf="$(wf_value "$_name")"
    if [[ -z "$_wf" ]]; then
        fail "${_name} is not defined in the client-test job's env: block"
        continue
    fi
    case "$_wf" in
    */* | *'${{'*)
        fail "${_name} is '${_wf}': it must be a bare name, because only the runner knows where the workspace is inside the el9 container"
        ;;
    esac
    if [[ "$(unjoined_uses "$_name")" != "0" ]]; then
        fail "${_name} is read without a \$GITHUB_WORKSPACE/ join somewhere in the client-test job"
    fi
done
ok

# The loop above is scoped to four names inside one job, which is the shape of
# the defect it caught rather than the shape of the invariant. Three ways back
# out of that box were all confirmed green: the same mistake in the `build` job
# (which has an el9 container leg of its own), a non-env path such as the
# package directory inside `client-test`, and a fifth env entry -- outside a
# literal list by construction.
#
# Both jobs in this file declare a container, so the prohibition is file-wide
# here. Scoping it to jobs that declare `container:` would be the faithful
# general rule and wants a YAML parse rather than a grep; do that if a
# container-free job is ever added to this workflow. Comments are exempt, or the
# env: block's own explanation of why the names are bare trips its own rule.
case_name="no step in this workflow interpolates the host workspace path"
failures_mark="$failures"

_offenders="$({ grep -nE '[$][{][{] *github\.workspace *[}][}]' "$WORKFLOW" || true; } |
    { grep -vE '^[0-9]+: *#' || true; })"
if [[ -n "$_offenders" ]]; then
    fail "\${{ github.workspace }} is interpolated on the HOST, and every job here has an el9 container leg where the workspace is bind-mounted elsewhere; use a bare name joined with \$GITHUB_WORKSPACE container-side -- ${_offenders}"
fi
ok

case_name="the test step writes its sentinel when make fails"
failures_mark="$failures"

work="$(mktemp -d "${SANDBOX}/case.XXXXXX")"
mkdir -p "${work}/bin" "${work}/cwd"

extract_run_block "Run the C client tests" >"${work}/step.sh"

if [[ -z "$(wf_value TEST_SENTINEL)" || -z "$(wf_value TEST_LOG)" ]]; then
    fail "skipped: the client-test job does not define TEST_LOG/TEST_SENTINEL (see above)"
elif [[ ! -s "${work}/step.sh" ]]; then
    fail "could not extract the 'Run the C client tests' run: block from ${WORKFLOW}"
else
    # Stand in for the client's make: report one failed case, then fail as make
    # does. This is the exact shape that used to end the step early.
    cat >"${work}/bin/make" <<'MAKE'
#!/usr/bin/env bash
echo "[x] string: 21/22 tests passed."
echo "22 tests: 21 passed, 1 failed"
exit 2
MAKE
    chmod +x "${work}/bin/make"

    # The job-level env: block, not a hand-written copy of it. The step writes
    # to these names, so reading them from the workflow is what makes the
    # assertions below mean anything -- and they go into the environment as the
    # bare names CI passes, so the step's own $GITHUB_WORKSPACE joins are what
    # decide where the files land.
    test_sentinel_name="$(wf_value TEST_SENTINEL)"
    test_log_name="$(wf_value TEST_LOG)"
    sentinel="${work}/${test_sentinel_name}"
    testlog="${work}/${test_log_name}"

    (
        cd "${work}/cwd" || exit 99
        export PATH="${work}/bin:${PATH}"
        export GITHUB_WORKSPACE="$work"
        export TEST_LOG="$test_log_name"
        export TEST_SENTINEL="$test_sentinel_name"
        # Exactly how the runner invokes a `shell: bash` step.
        bash --noprofile --norc -e -o pipefail "${work}/step.sh"
    ) >"${work}/out.txt" 2>&1 && printf 0 >"${work}/rc" || printf '%s' "$?" >"${work}/rc"

    expect_rc 2 # make's status, not tee's and not 1
    if [[ ! -f "$sentinel" ]]; then
        fail "the sentinel was not written, so a counted failure would be graded as a kill"
    elif [[ "$(cat "$sentinel")" != "2" ]]; then
        fail "the sentinel holds '$(cat "$sentinel")', expected make's status 2"
    fi
    grep -q "21/22" "$testlog" || fail "the tee'd log is missing make's output"
    ok
fi

# --- Cases: what the workflow has to uphold -----------------------------------------
#
# Everything above is on the reporter's side of a seam. Two fixes on the
# WORKFLOW's side of it were pinned at neither end -- `grep -n
# outputs.conclusion` returned no hits in this file -- so a straight revert of
# either, and adding continue-on-error to the reporter step, were all confirmed
# green. Asserted on the semantic token rather than a whole line: string
# assertions over YAML are brittle to reformatting, and unjoined_uses() already
# demonstrates that failure mode by rejecting a correct braced join.
#
# Comments are stripped first. Several of them quote the very expressions these
# cases prohibit, in the course of explaining why.

echo "What the workflow has to uphold:"

CLIENT_TEST_CODE="$(printf '%s\n' "$CLIENT_TEST_JOB" | grep -v '^ *#' || true)"

case_name="the log upload gates on the published grade, not the step outcome"
failures_mark="$failures"

grep -qF "steps.report.outputs.conclusion != 'success'" <<<"$CLIENT_TEST_CODE" ||
    fail "the artifact upload does not gate on steps.report.outputs.conclusion"
if grep -qF "steps.report.outcome" <<<"$CLIENT_TEST_CODE"; then
    fail "something reads steps.report.outcome, which collapses success and neutral onto 0 -- blind to every leg that died before the client build, which is exactly where asd.log is the only evidence"
fi
ok

# continue-on-error here turns every red leg green while .github/ci/README.md
# still says a failing test case fails that leg, with nothing red to contradict
# it. The reporter's exit status IS the leg's conclusion by design.
case_name="the reporter step is not continue-on-error"
failures_mark="$failures"

_report_step="$(awk '
    /^      - name: Report client test outcome$/ { in_step = 1; next }
    in_step && /^      - name: / { exit }
    in_step { print }
' "$WORKFLOW")"

if [[ -z "$_report_step" ]]; then
    fail "could not find the 'Report client test outcome' step in ${WORKFLOW}"
elif grep -qF "continue-on-error" <<<"$_report_step"; then
    fail "the reporter step is continue-on-error, so the grade no longer decides the leg"
fi
ok

# github.sha on a pull_request is the MERGE commit: the check run lands on a
# commit nobody is looking at and never appears in the PR's list. The transport
# case above asserts only that the reporter FORWARDS whatever it is handed, so
# without this nothing pins what the workflow hands it.
case_name="the check run hangs off the PR head, not the merge commit"
failures_mark="$failures"

_head_sha="$(wf_step_value HEAD_SHA)"
if [[ "$_head_sha" != '${{ github.event.pull_request.head.sha || github.sha }}' ]]; then
    fail "HEAD_SHA is '${_head_sha:-<absent>}', expected \${{ github.event.pull_request.head.sha || github.sha }}"
fi
ok

# --- Cases: the names arrive workspace-relative --------------------------------------
#
# in_workspace() is the container-side half of the bare-name contract above, and
# every case up to here hands it an ABSOLUTE path, so only its passthrough arm
# has ever run. Deleting the join left the suite green; so did swapping its
# operands, which in CI grades every leg on both distros 'could not parse the
# client's test summary', permanently, because the reporter then looks for
# '<name>/<workspace>'. These two hand over what CI hands over -- the workflow's
# own bare names, resolved by the reporter and not by this harness, from a cwd
# that is not the workspace.

echo "Workspace-relative inputs:"

relative_fixture=""

# Copy a fixture in under the name the WORKFLOW gives it, so the case is
# anchored to the env: block it mirrors rather than to a second copy of the
# literals.
plant_relative_names() {
    cp "${FIXTURES}/${relative_fixture}.log" "${work}/$(wf_value TEST_LOG)"
    if [[ -f "${FIXTURES}/${relative_fixture}.done" ]]; then
        cp "${FIXTURES}/${relative_fixture}.done" "${work}/$(wf_value TEST_SENTINEL)"
    fi
}

relative_case() {
    relative_fixture="$2"
    case_setup=plant_relative_names
    run_case "$1" "$2" \
        "TEST_LOG=$(wf_value TEST_LOG)" \
        "TEST_SENTINEL=$(wf_value TEST_SENTINEL)" \
        "${@:3}"
}

relative_case "a bare log name resolves against the workspace" clean TEST_OUTCOME=success
expect_rc 0
expect_conclusion success
expect_summary "all 2 suites passed"
ok

# The same join on a leg that has something to say, so the arm is pinned for a
# graded result and not only for the green path.
relative_case "a bare log name resolves for a graded failure too" five_failures \
    TEST_OUTCOME=failure
expect_rc 1
expect_conclusion failure
expect_summary "5 failed test cases"
ok

# --- Cases: the readiness probe -------------------------------------------------------
#
# 'Wait for the server to be ready' is what makes the "server must start" gate
# real, and until now nothing exercised it: the suite only ever extracted the
# test step, and live CI never reached this one because `build` was red. Its
# asd_dead() is a near-copy of the reporter's asd_alive() at the OPPOSITE
# polarity -- unreadable /proc means *dead* here and *alive* there, each
# correct for its own caller and each one edit away from the other. A wrong
# polarity is either an instant "asd died during start" on every leg or a 180s
# timeout blaming a slow start on every leg, so both directions are asserted.
#
# The step's own script is extracted from the workflow and run for real, so
# this cannot drift from what CI executes.

echo "The readiness probe:"

# Both wrong polarities fail by running the loop to exhaustion -- 180s for the
# no-marker arm -- rather than by answering wrongly and fast. Cap the step where
# a timeout(1) exists so that reads as a hung probe instead of as the suite
# stalling. macOS has no timeout(1); there the case just waits.
PROBE_TIMEOUT="$(command -v timeout || true)"

probe_case() {
    local desc="$1" pidfile="$2" logbody="$3"
    case_name="$desc"
    failures_mark="$failures"

    work="$(mktemp -d "${SANDBOX}/case.XXXXXX")"
    extract_run_block "Wait for the server to be ready" >"${work}/step.sh"

    if [[ ! -s "${work}/step.sh" ]]; then
        fail "could not extract the 'Wait for the server to be ready' run: block from ${WORKFLOW}"
        return
    fi

    local asd_log asd_pid
    asd_log="$(wf_value ASD_LOG)"
    asd_pid="$(wf_value ASD_PID_FILE)"

    # The drift case above owns this contract; bail readably rather than dying
    # on a redirect into a directory the sandbox does not have.
    if [[ "$asd_log" == */* || "$asd_pid" == */* ]]; then
        fail "ASD_LOG/ASD_PID_FILE are not bare names, so the probe cannot be sandboxed (see above)"
        return
    fi

    printf '%s' "$logbody" >"${work}/${asd_log}"
    cp "$pidfile" "${work}/${asd_pid}"

    (
        export GITHUB_WORKSPACE="$work"
        export ASD_LOG="$asd_log"
        export ASD_PID_FILE="$asd_pid"
        if [[ -n "$PROBE_TIMEOUT" ]]; then
            "$PROBE_TIMEOUT" 30 bash --noprofile --norc -e -o pipefail "${work}/step.sh"
        else
            bash --noprofile --norc -e -o pipefail "${work}/step.sh"
        fi
    ) >"${work}/out.txt" 2>&1 && printf 0 >"${work}/rc" || printf '%s' "$?" >"${work}/rc"
}

# The `[[ -r /proc/$1/stat ]] || return 0` arm. A pid that cannot be read is
# DEAD to this caller -- inverting it costs the whole fast path and turns every
# real death into the 180s timeout the probe exists to avoid.
probe_case "an unreadable pid is reported as a death, at once" \
    "${FIXTURES}/dead.pid" \
    "cold start in progress
"
expect_rc 1
expect_out "::error title=Aerospike server died during start::"
expect_out "cold start in progress" # the log tail, which is the whole point
ok

if [[ -d /proc ]]; then
    python3 - "${FIXTURES}/probe-zombie.pid" <<'ZOMBIE' &
import os, sys, time

pid = os.fork()
if pid == 0:
    os._exit(0)
with open(sys.argv[1], "w") as fh:
    fh.write("%d\n" % pid)
time.sleep(60)
ZOMBIE
    probe_zombie_parent=$!

    probe_zombie_state=""
    for _ in $(seq 1 100); do
        if [[ -s "${FIXTURES}/probe-zombie.pid" ]]; then
            probe_zombie_state="$(sed -nE 's/^.*\) ([A-Za-z]).*/\1/p' \
                "/proc/$(cat "${FIXTURES}/probe-zombie.pid")/stat" 2>/dev/null || true)"
            [[ "$probe_zombie_state" == "Z" ]] && break
        fi
        sleep 0.1
    done

    # The reason this is not `kill -0`: a job container's PID 1 is
    # `tail -f /dev/null` with no --init, so it never wait()s and a dead asd
    # stays a zombie for the rest of the leg on all three el9 legs.
    if [[ "$probe_zombie_state" == "Z" ]]; then
        probe_case "a zombie asd is reported as a death, not waited on" \
            "${FIXTURES}/probe-zombie.pid" "cold start in progress
"
        expect_rc 1
        expect_out "::error title=Aerospike server died during start::"
        ok
    else
        case_name="a zombie asd is reported as a death, not waited on"
        failures_mark="$failures"
        fail "could not produce a zombie to test against (state '${probe_zombie_state:-none}')"
    fi

    kill "$probe_zombie_parent" 2>/dev/null || true
    wait "$probe_zombie_parent" 2>/dev/null || true
else
    skip_or_fail "a zombie asd is reported as a death, not waited on" \
        "no /proc on this host"
fi

# The success path, which no negative case constrains: a live pid, the marker
# in the log, and something answering on the port the config binds. Skipped
# rather than failed where 3000 is already taken -- a developer running asd
# locally must not read as a broken probe.
if [[ -d /proc ]] && python3 -c 'import socket,sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", 3000))
except OSError:
    sys.exit(1)
' 2>/dev/null; then
    python3 - <<'LISTEN' &
import socket, time

s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 3000))
s.listen(8)
time.sleep(60)
LISTEN
    probe_listener=$!

    sleep 1
    printf '%s\n' "$probe_listener" >"${FIXTURES}/probe-live.pid"

    probe_case "a live asd that logged the marker and answers the port passes" \
        "${FIXTURES}/probe-live.pid" \
        "loading storage
service ready: soon may the milkman come
"
    expect_rc 0
    expect_out "asd reports service ready."
    expect_out "accepting connections on 127.0.0.1:3000"
    ok

    kill "$probe_listener" 2>/dev/null || true
    wait "$probe_listener" 2>/dev/null || true
else
    skip_or_fail "a live asd that logged the marker and answers the port passes" \
        "no /proc, or 127.0.0.1:3000 is already in use"
fi

# Not covered, deliberately: the 180s no-marker timeout and the 30s
# marker-but-no-port arm. Both are the loop running to exhaustion, and neither
# duration is worth spending on every commit.

# --- Cases: the caller's interface ---------------------------------------------------
#
# Four of the eleven variables this script requires come from the runner rather
# than from the workflow, so they appear nowhere in build-and-test.yaml and are
# easy to miss when running it by hand. Under set -u the first one aborted with
# a bare 'RUNNER_TEMP: unbound variable', naming a line instead of the contract.

echo "The caller's interface:"

case_name="a missing runner variable names itself"
failures_mark="$failures"
work="$(mktemp -d "${SANDBOX}/case.XXXXXX")"
: >"${work}/payload.json"

(
    env -i "PATH=${PATH}" \
        REF=7.5.0 DISTRO=el9 BUILD_OUTCOME=success TEST_OUTCOME=success \
        HEAD_SHA=0123456789abcdef0123456789abcdef01234567 CHECKS_TOKEN=x \
        RUN_URL=https://github.invalid/run/1 \
        "$BASH_BIN" "$REPORTER"
) >"${work}/out.txt" 2>&1 && printf 0 >"${work}/rc" || printf '%s' "$?" >"${work}/rc"

expect_rc 2
expect_out "RUNNER_TEMP is not set"
ok

# BUILD_OUTCOME and TEST_OUTCOME are legitimately EMPTY on a leg whose step
# never ran -- a state grade() reports rather than an error -- so the check
# above must test set-ness, not non-emptiness. Covered by 'a leg that never
# started is grey and passes', which exports BUILD_OUTCOME= and expects rc 0.

# --- Result -------------------------------------------------------------------------

# skip_or_fail() makes an unmet HOST gate a failure in CI. This catches a case
# that stops running for any other reason -- an `if` that grew a false arm, a
# run_case commented out, a section that returns early -- which the exit status
# alone cannot see, because a case that never runs fails no assertion. Exact
# rather than a floor, so a duplicated case is caught too. Bump it when you add
# one; the message says so.
EXPECTED_CASES=53

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} assertion(s) failed"
    exit 1
fi

if [[ -n "${CI:-}${GITHUB_ACTIONS:-}" ]]; then
    if [[ "$cases" -ne "$EXPECTED_CASES" ]]; then
        echo "${cases} cases ran, expected ${EXPECTED_CASES}: a case stopped running, or one was added or duplicated without updating EXPECTED_CASES"
        exit 1
    fi
elif [[ "$cases" -ne "$EXPECTED_CASES" ]]; then
    echo "${cases} of ${EXPECTED_CASES} cases ran; the rest are gated on this host (CI requires all ${EXPECTED_CASES})"
fi

echo "All report_client_test.bash cases passed."
