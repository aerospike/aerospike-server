#!/usr/bin/env bash
# Compute changelog markdown for Slack: commits since last successful workflow run
# (or fallbacks). Writes multiline "body" to GITHUB_OUTPUT for steps.changelog.outputs.body
#
# Required env (GitHub Actions): GITHUB_OUTPUT, GH_TOKEN, WORKFLOW_REF, BRANCH,
#   CURRENT_RUN_ID, CURRENT_SHA, MAX_COMMITS
# Optional: PUSH_BEFORE (github.event.before on push events)
# Optional: RUN_LIST_LIMIT (default 50) — how many recent workflow runs `gh run list` scans
# Optional: GITHUB_SERVER_URL, GITHUB_REPOSITORY — linkify (#NNN) in subjects to PR URLs

: "${GITHUB_OUTPUT:?}"
: "${GH_TOKEN:?}"
: "${WORKFLOW_REF:?}"
: "${BRANCH:?}"
: "${CURRENT_RUN_ID:?}"
: "${CURRENT_SHA:?}"
: "${MAX_COMMITS:?}"

RUN_LIST_LIMIT="${RUN_LIST_LIMIT:-50}"

# format_commit_lines_from_shas <array_name> <max> <out_lines_var> <out_shown_var>
# Uses local git log only (no per-commit GitHub API calls).
format_commit_lines_from_shas() {
    local -n _shas=$1
    local _max=$2
    local -n _lines_out=$3
    local -n _shown_out=$4
    local sha subj author line
    _lines_out=""
    _shown_out=0
    for sha in "${_shas[@]}"; do
        if ((_shown_out >= _max)); then break; fi
        subj=$(git log -1 --pretty=format:'%s' "${sha}")
        author=$(git log -1 --pretty=format:'%an' "${sha}")
        line="• ${subj} by ${author} (${sha:0:9})"
        # Collapse any embedded newlines to spaces — one line per commit.
        line="${line//$'\n'/ }"
        _lines_out+="${line}"$'\n'
        _shown_out=$((_shown_out + 1))
    done
    _lines_out="${_lines_out%$'\n'}"
}

# Turn squash-merge "(#123)" in commit subjects into Slack mrkdwn PR links (no per-commit API calls).
linkify_pr_refs_in_text() {
    local text="$1"
    if [[ -z "${GITHUB_REPOSITORY:-}" ]]; then
        printf '%s' "$text"
        return 0
    fi
    local repo_url="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY}"
    local result="" line rest prefix num built
    while IFS= read -r line || [[ -n "$line" ]]; do
        rest="$line"
        built=""
        while [[ "$rest" == *"(#"* ]]; do
            prefix="${rest%%(\#*}"
            rest="${rest#"$prefix"}"
            if [[ "$rest" =~ ^\(#([0-9]+)\) ]]; then
                num="${BASH_REMATCH[1]}"
                rest="${rest#(\#"${num}")}"
                built+="${prefix}(<${repo_url}/pull/${num}|#${num}>)"
            else
                # Not a numeric (#N) — keep the literal "(#" and keep scanning the rest.
                built+="${prefix}(#"
                rest="${rest#(#}"
            fi
        done
        built+="$rest"
        [[ -n "$result" ]] && result+=$'\n'
        result+="$built"
    done <<<"$text"
    printf '%s' "$result"
}

# Safe default so Slack never receives an empty mrkdwn text field if this script exits early.
# MUST precede `set -euo pipefail` so an early death still leaves a body set.
echo "body=_Changelog unavailable_" >>"$GITHUB_OUTPUT"
set -euo pipefail

# Use workflow file basename (stable) — not github.workflow display name — for gh run list.
path="${WORKFLOW_REF%@*}"
workflow_file="${path#*\.github/workflows/}"

# Most recent previous successful run of this workflow on this branch (excludes current run).
# Requires `actions: read` permission for `gh run list`; without it, the API returns 403.
prev_sha=$(gh run list \
    --workflow "${workflow_file}" \
    --branch "${BRANCH}" \
    --status success \
    --limit "${RUN_LIST_LIMIT}" \
    --json databaseId,headSha \
    --jq "[.[] | select(.databaseId != ${CURRENT_RUN_ID})] | .[0].headSha // empty" ||
    true)

if [[ -n "${prev_sha}" && ! "${prev_sha}" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "::warning::prev_sha from gh run list is not a valid 40-char SHA: '${prev_sha}'"
    prev_sha=""
fi

changelog_mode="last_successful_run"
if [[ -z "${prev_sha}" ]]; then
    echo "::warning::No previous successful run in last ${RUN_LIST_LIMIT} for 'gh run list --workflow=${workflow_file} --branch=${BRANCH}'; using fallback baseline so this run's commits are still listed"
    if [[ -n "${PUSH_BEFORE:-}" && "$PUSH_BEFORE" =~ ^[0-9a-fA-F]{40}$ && "$PUSH_BEFORE" != "0000000000000000000000000000000000000000" ]]; then
        prev_sha="${PUSH_BEFORE}"
        changelog_mode="push_before"
    fi
fi
if [[ -z "${prev_sha}" ]]; then
    git fetch origin master --no-tags 2>/dev/null || true
    mb=$(git merge-base origin/master "${CURRENT_SHA}" 2>/dev/null || true)
    if [[ -n "${mb}" && "$mb" != "${CURRENT_SHA}" ]]; then
        prev_sha="${mb}"
        changelog_mode="merge_base_origin_master"
    fi
fi
if [[ -z "${prev_sha}" ]]; then
    parent=$(git rev-parse -q --verify "${CURRENT_SHA}^" 2>/dev/null || true)
    if [[ -n "${parent}" ]]; then
        prev_sha="${parent}"
        changelog_mode="parent_commit"
    fi
fi

emit_body() {
    local delimiter
    delimiter="$(bash "$(dirname "${BASH_SOURCE[0]}")/random_output_delimiter.sh" CL)"
    {
        echo "body<<${delimiter}"
        printf '%s\n' "$1"
        echo "${delimiter}"
    } >>"$GITHUB_OUTPUT"
}

if [[ -z "${prev_sha}" ]]; then
    echo "::warning::Changelog: could not determine any baseline SHA; listing commit(s) on HEAD"
    short_prev="unknown"
    mapfile -t shas < <(git rev-parse "${CURRENT_SHA}" 2>/dev/null || true)
    total="${#shas[@]}"
    if ((total == 0)); then
        echo "::notice::Changelog mode=${changelog_mode} baseline=${short_prev}"
        emit_body "*Changelog:* _could not resolve HEAD (${CURRENT_SHA:0:9})_"
        exit 0
    fi
    lines=""
    shown=0
    format_commit_lines_from_shas shas "${MAX_COMMITS}" lines shown
    lines="$(linkify_pr_refs_in_text "$lines")"
    body="*Changelog* _(no baseline SHA could be resolved; listed ${shown} commit(s) on HEAD)_:"$'\n\n'"${lines}"
    if ((${#body} > 2900)); then
        body="${body:0:2900}"$'\n''_…truncated_'
    fi
    echo "::notice::Changelog mode=${changelog_mode} baseline=${short_prev}"
    echo "::notice::Changelog: emitted ${shown} commit(s) (no baseline fallback)"
    emit_body "${body}"
    exit 0
fi

# Ensure the previous SHA is in the local clone so prev_sha..HEAD resolves.
if ! git cat-file -e "${prev_sha}^{commit}" 2>/dev/null; then
    git fetch --no-tags --depth=200 origin "${prev_sha}" 2>/dev/null ||
        git fetch --no-tags origin "${prev_sha}" 2>/dev/null || true
fi
short_prev="${prev_sha:0:9}"
if ! git cat-file -e "${prev_sha}^{commit}" 2>/dev/null; then
    echo "::warning::Changelog: baseline ${short_prev} not in clone after fetch; listing recent commits on HEAD (includes current)"
    mapfile -t shas < <(git log --no-merges -n "${MAX_COMMITS}" --pretty=format:'%H' "${CURRENT_SHA}" 2>/dev/null || true)
    total="${#shas[@]}"
    if ((total == 0)); then
        echo "::notice::Changelog mode=${changelog_mode} baseline=${short_prev}"
        emit_body "*Changelog:* _could not list commits (no history?)_"
        exit 0
    fi
    lines=""
    shown=0
    format_commit_lines_from_shas shas "${MAX_COMMITS}" lines shown
    lines="$(linkify_pr_refs_in_text "$lines")"
    body="*Changelog* _(baseline \`${short_prev}\` unavailable; recent commits on branch)_:"$'\n\n'"${lines}"
    if ((${#body} > 2900)); then
        body="${body:0:2900}"$'\n''_…truncated_'
    fi
    echo "::notice::Changelog mode=${changelog_mode} baseline=${short_prev}"
    echo "::notice::Changelog: emitted ${shown} recent commit(s) (baseline missing)"
    emit_body "${body}"
    exit 0
fi

total=$(git rev-list --count --no-merges "${prev_sha}..${CURRENT_SHA}" 2>/dev/null || echo 0)

if ((total == 0)); then
    echo "::notice::Changelog mode=${changelog_mode} baseline=${short_prev} (no new commits)"
    emit_body "*Changelog* (since \`${short_prev}\`): _no new commits_"
    exit 0
fi

# One line per commit: subject, author, short SHA (squash-merge titles match PR titles on this repo).
lines=$(git log --no-merges -n "${MAX_COMMITS}" --pretty=format:'• %s by %an (%h)' \
    "${prev_sha}..${CURRENT_SHA}" 2>/dev/null || true)
lines="$(linkify_pr_refs_in_text "$lines")"
shown=$total
if ((shown > MAX_COMMITS)); then
    shown=$MAX_COMMITS
fi

# Blank line between header and list, one commit per line.
case "${changelog_mode}" in
last_successful_run) header_note="" ;;
push_before) header_note=" _(no prior green run in last ${RUN_LIST_LIMIT}; since push \`before\` SHA)_" ;;
merge_base_origin_master) header_note=" _(no prior green run in last ${RUN_LIST_LIMIT}; since merge-base with \`origin/master\`)_" ;;
parent_commit) header_note=" _(no prior green run in last ${RUN_LIST_LIMIT}; since parent of \`HEAD\`)_" ;;
*) header_note="" ;;
esac
body="*Changelog* (since \`${short_prev}\`)${header_note}:"$'\n\n'"${lines}"
if ((total > MAX_COMMITS)); then
    body+=$'\n'"_…and $((total - MAX_COMMITS)) more commit(s)_"
fi

# Slack enforces a 3 000-char cap on mrkdwn text fields; truncate to stay safely under it.
if ((${#body} > 2900)); then
    body="${body:0:2900}"$'\n''_…truncated_'
fi

echo "::notice::Changelog mode=${changelog_mode} baseline=${short_prev}"
echo "::notice::Changelog: emitted ${shown} of ${total} commit(s) since ${short_prev}"
emit_body "${body}"
