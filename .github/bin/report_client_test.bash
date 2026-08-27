#!/usr/bin/env bash
# Grade one build-and-test 'client-test' leg and publish the result.
#
# Lives here rather than in a run: block so that the repo's shfmt and
# pre-commit shellcheck hooks reach it -- both target shell *files*, so an
# embedded run: block is exempt from them -- so it can be run locally, and so
# test_report_client_test.bash can drive every branch from fixture logs instead
# of those branches only ever executing on a pushed six-leg matrix run. Same
# rationale as summarize_snyk_scan.sh.
#
# Usage: report_client_test.bash   (no arguments; the interface is the
#        environment map declared by the calling workflow step)
#
# Required:
#   REF             C client ref under test, e.g. '7.5.0' or 'stage'
#   DISTRO          distro the leg ran on, e.g. 'el9'
#   BUILD_OUTCOME   outcome of the 'Build the C client' step
#   TEST_OUTCOME    outcome of the 'Run the C client tests' step
#   HEAD_SHA        commit the check run hangs off
#   CHECKS_TOKEN    token with checks:write
#   RUN_URL         this workflow run, for details_url
#
# Also required, and supplied by the runner rather than by the workflow, so
# they are invisible in build-and-test.yaml and easy to miss when running this
# by hand:
#   RUNNER_TEMP           scratch outside the workspace the client can write to
#   GITHUB_STEP_SUMMARY   the job summary this appends to
#   GITHUB_API_URL        Checks API base
#   GITHUB_REPOSITORY     owner/name the check run is created on
#
# Optional. Workspace-relative names the workflow's job env: block owns, so
# that it, and not this file, is the authority on what they are called; joined
# with $GITHUB_WORKSPACE here, and an already-absolute value used as given:
#   TEST_LOG        tee'd 'make test' output
#   TEST_SENTINEL   written by the test step after make returns, whatever its
#                   status -- absent means the step was killed mid-run
#   ASD_PID_FILE    pid recorded by 'Start Aerospike server'
#
# Also optional, and not a path -- it is joined with nothing:
#   SHA             resolved C client commit; 'sha unknown' if unset
#
# Also optional, runner-supplied:
#   GITHUB_WORKSPACE  root the three names above are joined against; '.' if
#                     unset. The one variable the container fix hangs on.
#   GITHUB_OUTPUT     the grade is published here as `conclusion=<...>`, so a
#                     later step can gate on the grade itself rather than on
#                     this script's exit status, which deliberately collapses
#                     success and neutral onto 0
#
# Exit status is the grade: 0 for a clean leg and for one that never got to
# test, 1 for a test disagreement and for any state in which the count cannot
# be trusted -- see grade(). The job conclusion is the only surface a reader
# scanning the jobs list sees, and it has no third colour.

set -euo pipefail

# Fail on a misconfigured caller by name. Without this the first unbound
# reference aborts under set -u with a bare 'line 52: RUNNER_TEMP: unbound
# variable', which names a line rather than the interface. Set-ness, not
# non-emptiness: BUILD_OUTCOME and TEST_OUTCOME are legitimately empty on a leg
# whose step never ran, and that is a state grade() reports rather than an
# error.
for _var in REF DISTRO BUILD_OUTCOME TEST_OUTCOME HEAD_SHA CHECKS_TOKEN \
    RUN_URL RUNNER_TEMP GITHUB_STEP_SUMMARY GITHUB_API_URL GITHUB_REPOSITORY; do
    if [[ -z "${!_var+set}" ]]; then
        echo "::error::report_client_test.bash: ${_var} is not set"
        exit 2
    fi
done
unset _var

# The workflow hands these over as workspace-relative NAMES, because only the
# runner knows where the workspace is: a `${{ github.workspace }}` in the
# workflow is interpolated on the host, and the el9 legs run in a container that
# mounts it elsewhere. Join here, container-side. An already-absolute value is
# honoured as given, which is what a local run and the test suite pass.
in_workspace() {
    case "$1" in
    /*) printf '%s' "$1" ;;
    *) printf '%s/%s' "${GITHUB_WORKSPACE:-.}" "$1" ;;
    esac
}

TEST_LOG="$(in_workspace "${TEST_LOG:-client-tests.log}")"
TEST_SENTINEL="$(in_workspace "${TEST_SENTINEL:-client-tests-finished}")"
ASD_PID_FILE="$(in_workspace "${ASD_PID_FILE:-asd.pid}")"

CLEAN_LOG="${RUNNER_TEMP}/client-tests.clean.log"

# --- Transport ----------------------------------------------------------------------
#
# Nothing under this heading knows what a C client is: escaping, the Checks
# API, and the job summary. The policy section below decides what to say.

# Only operator-supplied or composed text needs this. check_name embeds REF,
# which on workflow_dispatch is a free-form element of the client_versions
# input, and title/summary are composed from it. head_sha, conclusion and
# details_url are fixed-shape -- a SHA, one of five literals, a runner-built
# URL -- and are interpolated raw. No jq: the el9 container image lacks it.
#
# Fold the three whitespace controls to spaces, drop every other C0 byte (ESC,
# BEL, NUL and friends are equally illegal raw inside a JSON string), then drop
# the two characters that would end the string early. The doubled backslash is
# tr's own escape rather than the shell's; keeping it away from the closing
# quote also keeps shellcheck's position-sensitive SC1003 quiet.
json_safe() {
    tr '\n\r\t' '   ' | tr -d '\000-\037' | tr -d '\\"'
}

# Print a curl config carrying the Authorization header, for `curl -K -`.
#
# Keeping the token out of argv narrows one reader -- /proc/<pid>/cmdline is
# mode 0444, so any uid on the box can scrape it while the call is in flight --
# and nothing more. Be exact about what is left: CHECKS_TOKEN is a step-level
# env: entry, so it is in /proc/<pid>/environ of this shell and of every child
# curl, readable by the same uid the third-party make ran as, for the whole
# step. That is the same no-write read, over a longer window. Only the deferred
# reporter-job split closes it -- see .github/ci/README.md.
#
# A third channel needs neither that read nor a workspace write. Every run:
# step is handed $GITHUB_PATH, and a line the client's own make appends to that
# file is prepended to PATH for every LATER step of the job -- including this
# one, whose interpreter and every tool it calls (curl, sed, tr, cat, readlink,
# and the `command -v curl` probe) are unqualified. A planted curl is handed
# the config below on stdin and holds CHECKS_TOKEN in its own environ anyway,
# so the argv-versus-stdin distinction above is moot against it. $GITHUB_ENV
# and $GITHUB_STEP_SUMMARY are the same file-command mechanism. Listed rather
# than patched: pointing the two client steps at scratch GITHUB_PATH/GITHUB_ENV
# files closes one spelling of five -- overwriting THIS file needs no PATH at
# all -- and doing it with a `${{ runner.temp }}` path would re-enter the
# host-vs-container bug the bare-name contract exists to prevent. The split is
# what closes the class.
#
# Through a pipe rather than a file in RUNNER_TEMP. A file would be a third
# no-write read (its path advertised by `-K <path>`, and it outlives the call),
# and a create-if-absent guard on it would honour a planted config -- `-K` sets
# any curl option, not just a header, so a planted one can redirect both calls
# and turn a published check run into a silent `::warning::`. Neither call site
# reads stdin: the lookup passes --data-urlencode literals, the write --data @file.
auth_config() {
    printf 'header = "Authorization: Bearer %s"\n' "$CHECKS_TOKEN"
}

# Look up this leg's check run on HEAD_SHA, if it already has one. Prints the
# id, or nothing. 'Re-run failed jobs' reuses HEAD_SHA, so without this a
# re-run adds a second check run of the same name carrying a different
# conclusion, and the checks list ends up disagreeing with itself.
existing_check_id() {
    local resp="${RUNNER_TEMP}/check-run-lookup.json" code

    code="$(auth_config | curl -sS -G -o "$resp" -w '%{http_code}' -K - \
        --data-urlencode "check_name=${check_name}" \
        "${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/commits/${HEAD_SHA}/check-runs" \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28")" || return 0

    [[ "$code" == "200" ]] || return 0

    # Scrape rather than parse: no jq here. Whitespace is stripped first so a
    # pretty-printed body reads the same as a compact one, and the anchor is
    # the array open, so total_count cannot be mistaken for an id.
    tr -d ' \n' <"$resp" | sed -nE 's/.*"check_runs":\[\{"id":([0-9]+).*/\1/p'
}

publish_check() {
    local conclusion="$1" title="$2" summary="$3"
    local method=POST url="${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/check-runs"
    local want=201 id code

    if ! command -v curl >/dev/null; then
        echo "::warning::curl not found; skipping the '${check_name}' check run."
        return 0
    fi

    title="$(printf '%s' "$title" | json_safe)"
    summary="$(printf '%s' "$summary" | json_safe)"

    id="$(existing_check_id)"
    if [[ -n "$id" ]]; then
        method=PATCH
        url="${url}/${id}"
        want=200
    fi

    cat >"${RUNNER_TEMP}/check-run.json" <<PAYLOAD
{
  "name": "${check_name}",
  "head_sha": "${HEAD_SHA}",
  "status": "completed",
  "conclusion": "${conclusion}",
  "details_url": "${RUN_URL}",
  "output": { "title": "${title}", "summary": "${summary}" }
}
PAYLOAD

    code="$(auth_config | curl -sS -o "${RUNNER_TEMP}/check-run-resp.json" -w '%{http_code}' \
        -X "$method" "$url" -K - \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        --data @"${RUNNER_TEMP}/check-run.json")" || code="000"

    if [[ "$code" == "$want" ]]; then
        echo "Published check '${check_name}': ${conclusion} — ${title}"
    else
        # Never fail over reporting: a fork PR's token is read-only, and losing
        # the colour is not worth losing the run.
        echo "::warning::could not publish the '${check_name}' check run (HTTP ${code}); the step summary still has the result."
        cat "${RUNNER_TEMP}/check-run-resp.json" 2>/dev/null || true
    fi
}

# The step summary is plain markdown through GitHub's sanitizer -- style
# attributes and CSS are stripped, so the only colour available is a GFM alert:
# TIP green, WARNING yellow, CAUTION red, NOTE blue.
summarize() {
    local alert="$1" headline="$2" body="$3"

    {
        echo "> [!${alert}]"
        echo "> **${headline}**"
        echo "> ${body}"
        echo
    } >>"${GITHUB_STEP_SUMMARY}"
}

# --- Policy -------------------------------------------------------------------------
#
# What counts as a failure and how bad it is. A pure function of the
# environment and the log, which is what makes it testable.

# A mutable ref can break for reasons that have nothing to do with this PR, so
# say which kind of failure the reader is looking at. Derived from the ref's
# shape rather than a matrix flag: a flag would need an `include:` row keyed on
# a specific client value, and such a row SPAWNS AN EXTRA JOB whenever that
# value is absent from the client list -- which the workflow_dispatch input
# makes routine.
#
# Anchored and fully specified so a pre-release suffix cannot read as shipped:
# 7.4.0 and v7.6.0 are releases; 7.6.0-rc1 and stage are not.
classify_ref() {
    if [[ "$1" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "shipped client — investigate the server change"
    else
        echo "pre-release client — upstream breakage is possible and may not be caused by this change"
    fi
}

# Strip the terminal control sequences and CRs the client's harness may emit
# before parsing, so the patterns below constrain content rather than
# formatting. Without this, a colour reset between the status glyph and the
# suite name -- or a CRLF line ending -- silently drops a suite from the count.
#
# Every CR, not just a trailing one: a progress-bar redraw leaves a bare CR
# mid-line, which would otherwise ride inside a suite name into the workflow
# command emitted by main(), where the runner treats a lone CR as a line break.
clean_test_log() {
    if [[ -f "$TEST_LOG" ]]; then
        sed -e 's/\x1b\[[0-9;]*[A-Za-z]//g' -e 's/\r//g' "$TEST_LOG" >"$CLEAN_LOG"
    else
        : >"$CLEAN_LOG"
    fi
}

# Count individual failed test cases, not failing suites, from the client's
# '<suite>: N/M tests passed.' lines. Sets suites, suite_failures and detail.
#
# The suite name is matched as [^:]+ rather than a character whitelist -- the
# format is ':'-delimited, so a negated class is both safer and more faithful,
# and over-counting fails safe where dropping a line does not.
#
# 10# on both counts before anything arithmetic touches them. The client owns
# this format and [0-9]+ admits a leading zero, which bash arithmetic reads as
# an octal literal: '09' is not a literal at all and aborts the whole grader
# from a loop BODY, where errexit is not exempt, so no surface gets a grade;
# '0100' is legal and silently worth 64. Same reason and same spelling as
# build-sign-deploy.yaml:109 and tag-release.yaml:164.
parse_suite_lines() {
    local passed total suite

    suites=0
    suite_failures=0
    detail=""

    while read -r passed total suite; do
        passed=$((10#$passed))
        total=$((10#$total))
        suites=$((suites + 1))
        suite_failures=$((suite_failures + total - passed))
        if [[ "$passed" -ne "$total" ]]; then
            detail="${detail:+${detail}; }${suite} ${passed}/${total}"
        fi
    done < <(sed -nE 's/.*[]] ([^:]+): ([0-9]+)\/([0-9]+) tests passed\.$/\2 \3 \1/p' "$CLEAN_LOG")
}

# The client prints a run total ('389 tests: 388 passed, 1 failed') after the
# per-suite block. Parsing it too gives the suite sum something to be checked
# against, so a change to either format shows up as their disagreement rather
# than silently halving the count. Sets total_failures; -1 when absent.
parse_total_line() {
    local parsed
    parsed="$(sed -nE 's/^[0-9]+ tests: [0-9]+ passed, ([0-9]+) failed\.?$/\1/p' "$CLEAN_LOG" | tail -n 1)"
    # Normalised once, here, so every comparison downstream operates on a
    # base-10 integer. It has to happen before the sentinel is chosen: 10# is an
    # error on an empty string and on -1 alike, and these comparisons sit in
    # CONDITION position, where errexit is exempt -- so a padded total made
    # `-ge 0` and `-lt 0` both read false, skipping the cross-check that exists
    # to catch a format drift and the "no total line" arm together, with two
    # bash errors in the step log as the only trace.
    total_failures="${parsed:+$((10#$parsed))}"
    total_failures="${total_failures:--1}"
}

# asd is started and probed before the client is even checked out, then never
# looked at again. A server that dies during the ~30 s client build takes the
# whole suite with it, and every 'kind' string below would blame the client.
#
# Deliberately not `kill -0`: that tests for a task_struct, which a zombie
# still has, and on the el9 legs asd is guaranteed to become one. GitHub
# creates a job container with `--entrypoint tail <image> -f /dev/null` and no
# `--init`, so PID 1 is tail, which never wait()s; steps run under `docker
# exec`, whose root process is the step's own shell rather than a reaping
# ancestor. An asd orphaned there stays in state Z for the life of the job, so
# `kill -0` would report it alive and this arm would be unreachable on half the
# matrix -- silently, and on the half nobody would guess.
#
# Read the state instead, and confirm the identity when /proc will say, so a
# pid recycled during the ~25 minutes since it was recorded cannot vouch for a
# server that is gone.
asd_alive() {
    local pid state exe

    [[ -f "$ASD_PID_FILE" ]] || return 0 # nothing recorded; do not claim a death
    pid="$(cat "$ASD_PID_FILE")"
    [[ -n "$pid" ]] || return 0

    [[ -r "/proc/${pid}/stat" ]] || return 1

    # Field 3, after the parenthesised comm -- which may itself contain spaces
    # and parens, so anchor on the LAST ')' rather than splitting on blanks.
    state="$(sed -nE 's/^.*\) ([A-Za-z]).*/\1/p' "/proc/${pid}/stat")"
    [[ "$state" != "Z" ]] || return 1

    # Best effort: /proc/<pid>/exe is unreadable under some sandboxes, and an
    # answer we could not get must not be read as a death.
    exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
    [[ -z "$exe" || "$exe" == */asd ]]
}

# Decide the leg's grade. Sets conclusion, partial, alert, mark, title, counted,
# body and detail -- every surface, and the exit status, derive from these.
#
# One decision. The earlier shape computed the colour from the parsed count and
# then re-decided the prose and the exit status from make's exit status, so a
# run whose count and status disagreed published a red check run whose body
# read 'every test passed' and let the job go green.
grade() {
    local failures

    detail=""
    partial="false"

    if [[ "$BUILD_OUTCOME" != "success" ]]; then
        # Neither of these fails the job: a client that will not compile in our
        # image says nothing about this server, and a leg that never got to
        # build has nothing to say at all.
        conclusion="neutral"
        alert="NOTE"
        if [[ "$BUILD_OUTCOME" == "failure" ]]; then
            mark="🔧"
            title="did not build"
            counted="failed to compile, so no tests ran"
            body="Usually a CI dependency gap rather than a server incompatibility. See the 'Build the C client' step log."
        else
            mark="⏭️"
            title="did not run"
            counted="was not exercised (build outcome '${BUILD_OUTCOME:-not-run}')"
            body="An earlier gating step failed."
        fi
        return
    fi

    clean_test_log
    parse_suite_lines
    parse_total_line

    # Over-count on disagreement. Each parser reads a format the client owns and
    # can change; taking the larger keeps a format drift from understating the
    # damage, and the warning names the drift so it gets fixed.
    failures="$suite_failures"
    if [[ "$total_failures" -ge 0 ]]; then
        # No `suites -gt 0` conjunct. With no suite lines suite_failures is 0,
        # so that guard only ever suppressed the loudest drift there is -- a
        # run total reporting failures that not one suite line accounted for.
        if [[ "$total_failures" -ne "$suite_failures" ]]; then
            echo "::warning::per-suite counts (${suite_failures}) and the run total (${total_failures}) disagree; grading on the larger. The client's summary format may have changed."
        fi
        [[ "$total_failures" -gt "$failures" ]] && failures="$total_failures"
    fi

    counted="${failures} failed test cases"
    [[ "$failures" -eq 1 ]] && counted="1 failed test case"

    # Ordered least-trustworthy-count first. Each arm above the count
    # thresholds names a state in which the count does not mean what it says,
    # so it must not be graded as though it did.
    if ! asd_alive; then
        conclusion="failure"
        title="the server died during the client tests"
        counted="the server was gone before the tests finished"
        body="Not a client/server disagreement. See asd.log in this leg's log artifact."
    elif [[ "$TEST_OUTCOME" == "failure" && ! -f "$TEST_SENTINEL" ]]; then
        # The test step writes the sentinel after make returns, whatever its
        # status, so a missing one means the step was killed -- a
        # timeout-minutes kill, or the runner going away. The log is truncated,
        # so whatever was counted is a lower bound on an unknown total.
        conclusion="failure"
        title="tests did not finish"
        counted="the test step was killed before make returned (timeout or runner loss)"
        body="The test step was killed before make returned, so any count here is a lower bound. See the 'Run the C client tests' step log."
    elif [[ "$suites" -eq 0 ]]; then
        # Zero suite lines is always a parse failure. The client emits the
        # per-suite block before its run total, so their absence is never
        # legitimate -- and this must NOT be conditioned on the total line
        # being absent too. With the total line present and parsing, the count
        # is 0 and the leg would otherwise grade green under the words 'all 0
        # suites passed': a per-suite format drift would turn every leg
        # permanently, silently green.
        conclusion="failure"
        title="could not parse the client's test summary"
        counted="no '<suite>: N/M tests passed.' line was found in the log"
        body="Either the run died before reporting, or the client changed its per-suite format. See the 'Run the C client tests' step log."
    elif [[ "$total_failures" -lt 0 ]]; then
        # Suite lines but no run total: the client stopped before summarising.
        # That is a segfaulting test binary, an OOM-killed child, or `make -k`
        # -- cases where make RETURNS, so the sentinel above cannot see them by
        # construction. The count is a lower bound on an unknown total, so it
        # must not be graded as the bounded result it looks like.
        conclusion="failure"
        title="the client's run did not reach its total line"
        counted="${suites} suites reported, but no run-total line followed them"
        body="The run stopped before summarising, so ${failures} is a lower bound. See the 'Run the C client tests' step log."
    elif [[ "$failures" -eq 0 && "$TEST_OUTCOME" != "success" ]]; then
        conclusion="failure"
        title="tests failed but every counted suite passed"
        counted="make test failed while all ${suites} reported suites passed"
        body="The failure is outside the per-suite counts. See the 'Run the C client tests' step log."
    elif [[ "$failures" -eq 0 ]]; then
        conclusion="success"
        title="all tests passed"
        counted="all ${suites} suites passed"
        body="Every suite reported a full pass."
    elif [[ "$failures" -lt 5 ]]; then
        # A handful of failures is worth telling apart from a wholesale break,
        # but only on a surface that has a third colour to spend. The check-run
        # conclusion does not: `action_required` was measured on this PR's own
        # check runs and `gh pr checks` prints it as `fail`, the commit's
        # statusCheckRollup is FAILURE, and once these checks are required it
        # blocks a merge exactly as `failure` does -- silently, with no change
        # here. It is also GitHub's conclusion for "the integrator needs the
        # user to press a button", and this payload carries no actions[] to
        # press. So the tier lives in the count, which is already in the title,
        # and in the job summary's GFM alert, which does render yellow.
        conclusion="failure"
        partial="true"
        title="$counted"
        body="$(classify_ref "$REF"). See the 'Run the C client tests' step log."
    else
        conclusion="failure"
        title="$counted"
        body="$(classify_ref "$REF"). See the 'Run the C client tests' step log."
    fi

    # Not a `case` on $conclusion: the alert has four values and the conclusion
    # has three, and the yellow is exactly where they stop agreeing.
    if [[ "$conclusion" == "success" ]]; then
        alert="TIP"
        mark="✅"
    elif [[ "$partial" == "true" ]]; then
        alert="WARNING"
        mark="⚠️"
    else
        alert="CAUTION"
        mark="❌"
    fi
}

# --- Main ---------------------------------------------------------------------------

main() {
    local label headline summary_line ref_safe sha_safe

    # These reach workflow commands, where a newline would start a command of
    # the attacker's choosing -- ::stop-commands:: among them, which would
    # suppress the very annotations this script exists to emit. json_safe is
    # the right filter for that sink as well as for a JSON string; only its
    # name suggests otherwise.
    #
    # SHA needs it as much as REF does. It arrives as the client-build step's
    # output, and that step runs the client's own make AFTER writing it -- with
    # GITHUB_OUTPUT in make's environment and step outputs last-write-wins, so
    # it is free text of the graded party's choosing, not the 40 hex bytes it
    # looks like.
    ref_safe="$(printf '%s' "$REF" | json_safe)"
    sha_safe="$(printf '%s' "${SHA:-sha unknown}" | json_safe)"

    # Identify the tree, not just the ref name: for a branch ref the name pins
    # nothing, so without the SHA two disagreeing runs produce identical lines.
    label="C client ${ref_safe} (${sha_safe})"
    check_name="$(printf '%s' "C client ${REF} (${DISTRO})" | json_safe)"

    grade

    # Filtered at the sink rather than input by input: detail is assembled from
    # the client's own suite names, which no filter upstream of here has seen.
    summary_line="$(printf '%s' "${label}: ${counted}${detail:+ — ${detail}}" | json_safe)"
    headline="${mark} ${summary_line}"

    case "$conclusion" in
    success) ;;
    neutral)
        echo "::warning title=${ref_safe} client ${title}::${body}"
        ;;
    *)
        echo "::error title=${ref_safe} client tests failed::${headline}. ${body}"
        ;;
    esac

    summarize "$alert" "$headline" "$body"
    publish_check "$conclusion" "$title" "${summary_line}. ${body}"

    # The grade itself, for steps that need to tell 'neutral' from 'success'.
    # The exit status below cannot: it collapses both onto 0 on purpose, so a
    # client that will not compile does not fail the job -- which also means
    # every leg that died BEFORE the client build exits 0 while grading grey.
    # A downstream `steps.report.outcome != 'success'` therefore reads false
    # across exactly the region where asd.log is the only evidence.
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        printf 'conclusion=%s\n' "$conclusion" >>"$GITHUB_OUTPUT"
    fi

    # The grade decides, so the three surfaces and the exit status cannot
    # disagree. neutral exits 0 deliberately -- see grade().
    case "$conclusion" in
    success | neutral) exit 0 ;;
    *) exit 1 ;;
    esac
}

main "$@"
