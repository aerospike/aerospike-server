#!/usr/bin/env bash
# check_clang_format_changes.bash — local clang-format check for *changed* C/C++ files only.
#
# Used by CI (.github/workflows/format.yaml) and as a local pre-push helper.
# Rules: git diff --name-only -z --diff-filter=ACMRT, suffix filter (.c .h .cc .cpp .hpp .cxx .inc),
#        then:  clang-format -style=file FILE  |  diff -u FILE -
#
# Quick examples (run from anywhere under the repo; script cds to repo root):
#
#   ./.github/bin/check_clang_format_changes.bash
#   ./.github/bin/check_clang_format_changes.bash --base origin/master --head HEAD
#   base="$(gh pr view 123 --json baseRefOid -q .baseRefOid)"
#   head="$(gh pr view 123 --json headRefOid -q .headRefOid)"
#   ./.github/bin/check_clang_format_changes.bash --base "$base" --head "$head"
#   CLANG_FORMAT=clang-format-18 ./.github/bin/check_clang_format_changes.bash
#
# Full usage and modes:  ./.github/bin/check_clang_format_changes.bash --help
#
# Exit: 0 ok, 1 formatting diffs, 2 usage/repo/tool errors.
# Fix (from repo root):  clang-format-18 -style=file -i <paths>
#
set -euo pipefail

usage() {
    cat <<'EOF' >&2
check_clang_format_changes.bash — scan *changed* C/C++ files with clang-format (same rules as CI).

SYNOPSIS
  check_clang_format_changes.bash
  check_clang_format_changes.bash --base REF [--head REF]
  check_clang_format_changes.bash BASE_REF HEAD_REF

OPTIONS
  --base REF   Left side of a three-dot diff: git diff REF...HEAD (see MODES).
  --head REF   Right side of the diff (default: HEAD).
  -h, --help   Print this message (includes examples).

ENVIRONMENT
  CLANG_FORMAT  Clang-format binary (default: clang-format-18 on PATH, else clang-format).

MODES
  Default (no --base, no positional args)
      Resolves upstream (first of origin/master, origin/main, refs/remotes/origin/HEAD), then:
        git diff --diff-filter=ACMRT 'UPSTREAM...HEAD'
      i.e. three-dot: changes on your branch since the merge-base with upstream (good before a PR).

  With --base REF and/or two positional args BASE_REF HEAD_REF
        git diff --diff-filter=ACMRT 'BASE...HEAD'
      Three-dot: changes on the head side since the merge-base with BASE -- i.e.
      only what this branch introduced. Same file set as CI (and as GitHub's
      "Files changed" tab) when BASE and HEAD are the PR base and head OIDs.
      Three-dot matters because the PR base OID tracks the moving base-branch
      tip: a two-dot diff would wrongly include files the base branch changed
      after this branch was cut.

EXAMPLES
  # 1) Default: your work vs upstream (three-dot; closest to “my PR diff” before you open one):
  ./.github/bin/check_clang_format_changes.bash

  # 2) Explicit base (three-dot): what your branch introduced since it diverged from master:
  ./.github/bin/check_clang_format_changes.bash --base origin/master --head HEAD

  # 3) Match the PR check for GitHub PR #123 (same base/head commits the workflow uses):
  base="$(gh pr view 123 --json baseRefOid -q .baseRefOid)"
  head="$(gh pr view 123 --json headRefOid -q .headRefOid)"
  ./.github/bin/check_clang_format_changes.bash --base "$base" --head "$head"

  # 4) Same as (3), positional:
  ./.github/bin/check_clang_format_changes.bash "$base" "$head"

  # 5) Match CI’s clang-format binary name on Ubuntu:
  CLANG_FORMAT=clang-format-18 ./.github/bin/check_clang_format_changes.bash

See also: .github/workflows/format.yaml
EOF
    exit "$1"
}

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
    echo "error: not inside a git repository" >&2
    exit 2
}
cd "$repo_root"

resolve_upstream() {
    for candidate in origin/master origin/main; do
        if git rev-parse -q --verify "$candidate" >/dev/null 2>&1; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    if sym="$(git symbolic-ref -q refs/remotes/origin/HEAD 2>/dev/null)"; then
        printf '%s' "$sym"
        return 0
    fi
    echo "error: could not find origin/master, origin/main, or origin/HEAD; pass --base explicitly" >&2
    exit 2
}

base_ref=""
head_ref="HEAD"
while [[ $# -gt 0 ]]; do
    case "$1" in
    -h | --help) usage 0 ;;
    --base)
        base_ref="${2:-}"
        [[ -n "$base_ref" ]] || {
            echo "error: --base needs a value" >&2
            exit 2
        }
        shift 2
        ;;
    --head)
        head_ref="${2:-}"
        [[ -n "$head_ref" ]] || {
            echo "error: --head needs a value" >&2
            exit 2
        }
        shift 2
        ;;
    --)
        shift
        break
        ;;
    -*)
        echo "error: unknown option: $1" >&2
        usage 2
        ;;
    *)
        break
        ;;
    esac
done

if [[ $# -eq 2 ]]; then
    base_ref="$1"
    head_ref="$2"
    shift 2
fi
if [[ $# -ne 0 ]]; then
    echo "error: unexpected arguments: $*" >&2
    usage 2
fi

if [[ -z "$base_ref" ]]; then
    upstream="$(resolve_upstream)"
    base_ref="$upstream"
    # Three-dot: changes on head side since merge-base with upstream (PR-like file set).
    diff_range="${base_ref}...${head_ref}"
    echo "Using diff range: ${diff_range}"
else
    # Three-dot: changes on the head side since the merge-base with base. The
    # PR base OID tracks the moving base-branch tip, so two-dot would pull in
    # files the base branch changed after this branch was cut (not PR changes).
    diff_range="${base_ref}...${head_ref}"
    echo "Using diff range: ${diff_range}"
fi

if ! git rev-parse -q --verify "$head_ref" >/dev/null 2>&1; then
    echo "error: bad head ref: $head_ref" >&2
    exit 2
fi
# Also validate the base side of the three-dot range
if [[ "$diff_range" == *"..."* ]]; then
    base_side="${diff_range%%...*}"
    if ! git rev-parse -q --verify "$base_side" >/dev/null 2>&1; then
        echo "error: bad base ref for three-dot range: $base_side" >&2
        exit 2
    fi
else
    if ! git rev-parse -q --verify "${diff_range%%..*}" >/dev/null 2>&1; then
        echo "error: bad base ref: ${diff_range%%..*}" >&2
        exit 2
    fi
fi

cf="${CLANG_FORMAT:-}"
if [[ -z "$cf" ]]; then
    if command -v clang-format-18 >/dev/null 2>&1; then
        cf="clang-format-18"
    elif command -v clang-format >/dev/null 2>&1; then
        cf="clang-format"
    else
        echo "error: no clang-format-18 or clang-format in PATH; install one or set CLANG_FORMAT" >&2
        exit 2
    fi
fi

is_c_like() {
    case "$1" in
    *.c | *.h | *.cc | *.cpp | *.hpp | *.cxx | *.inc) return 0 ;;
    *) return 1 ;;
    esac
}

declare -a files=()
_tmplist="$(mktemp)"
trap 'rm -f "$_tmplist"' EXIT
git diff --name-only -z --diff-filter=ACMRT "$diff_range" >"$_tmplist"
while IFS= read -r -d '' path; do
    if is_c_like "$path"; then
        files+=("$path")
    fi
done <"$_tmplist"
rm -f "$_tmplist"
trap - EXIT

if [[ ${#files[@]} -eq 0 ]]; then
    echo "No C/C++ sources in diff range; nothing to check."
    exit 0
fi

echo "Checking ${#files[@]} file(s) with ${cf} -style=file (repository .clang-format)."

exit_code=0
declare -a failed_files=()
tmpdiff=""
trap '[ -n "$tmpdiff" ] && rm -f "$tmpdiff"' EXIT
for f in "${files[@]}"; do
    if [[ ! -f "$f" ]]; then
        echo "warning: skipping missing path: $f" >&2
        continue
    fi
    tmpdiff="$(mktemp)"
    set +e
    "$cf" -style=file "$f" | diff -u "$f" --label "$f" - --label "$f (formatted)" >"$tmpdiff"
    _pipe_status=("${PIPESTATUS[@]}")
    set -e
    cf_rc=${_pipe_status[0]}
    diff_rc=${_pipe_status[1]}
    if [[ "$cf_rc" -ne 0 ]]; then
        echo "error: $cf failed on $f (exit $cf_rc)" >&2
        rm -f "$tmpdiff"
        tmpdiff=""
        exit 2
    fi
    if [[ "$diff_rc" -eq 2 ]]; then
        echo "error: diff failed on $f (I/O or tool error)" >&2
        rm -f "$tmpdiff"
        tmpdiff=""
        exit 2
    fi
    if [[ "$diff_rc" -ne 0 ]]; then
        echo "format-diff: $cf would rewrite $f (unified diff; + = expected formatted line)" >&2
        echo "---------- begin diff: $f ----------" >&2
        cat "$tmpdiff" >&2
        echo "---------- end diff: $f ----------" >&2
        failed_files+=("$f")
        exit_code=1
    fi
    rm -f "$tmpdiff"
    tmpdiff=""
done
trap - EXIT

if [[ "$exit_code" -ne 0 ]]; then
    echo "clang-format check failed for ${#failed_files[@]} file(s)." >&2
    echo "From the repository root, apply the same style as CI with:" >&2
    printf '  %s -style=file -i' "$cf" >&2
    printf ' %q' "${failed_files[@]}" >&2
    echo >&2
fi

exit "$exit_code"
