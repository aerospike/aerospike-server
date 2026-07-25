#!/usr/bin/env python3
# check_comment_style_changes.py - flag C-style /* */ comments a PR introduces.
#
# Convention (CLAUDE.md): after the leading license header, every comment must
# be C++ style //, never C style /* */. clang-format cannot enforce this - it
# never rewrites comment delimiters.
#
# Used by CI (.github/workflows/format.yaml) and as a local pre-push helper.
# Like check_clang_format_changes.bash, it looks at *changed* C/C++ files only,
# and - because the tree carries legacy /* */ headers and bodies - it flags a
# comment only when the comment's line was ADDED in this diff. A PR is never
# blamed for a /* */ it merely sits next to.
#
# The license header is the /* ... */ block at the very top of the file; it is
# exempt. Everything after it that adds a line containing /* is a violation.
#
# Usage:
#   check_comment_style_changes.py [--base REF] [--head REF]
#   --base REF   left side of a three-dot diff REF...HEAD (default: origin/master)
#   --head REF   right side of the diff (default: HEAD)
#
# Three-dot (REF...HEAD) diffs HEAD against the merge-base with REF, so only the
# lines this branch introduced are considered. The PR base OID tracks the moving
# base-branch tip; a two-dot diff would pull in files the base branch changed
# after this branch was cut, which the PR did not touch.
# Exit: 0 ok, 1 a new /* */ comment was introduced, 2 usage/repo/tool errors.

import subprocess
import sys

SUFFIXES = (".c", ".h", ".cc", ".cpp", ".hpp", ".cxx", ".inc")


def git(*args):
    r = subprocess.run(
        ["git", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if r.returncode != 0:
        sys.exit("error: git %s failed: %s" % (" ".join(args), r.stderr.strip()))
    return r.stdout


def git_optional(*args):
    # Like git(), but returns None instead of exiting when the command fails
    # (e.g. `git show HEAD:path` for a path absent on the head side).
    r = subprocess.run(
        ["git", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    return r.stdout if r.returncode == 0 else None


def parse_args(argv):
    base, head = None, "HEAD"
    it = iter(argv)
    for a in it:
        if a == "--base":
            base = next(it, None)
        elif a == "--head":
            head = next(it, None) or "HEAD"
        else:
            sys.exit("usage: check_comment_style_changes.py [--base REF] [--head REF]")
    return base, head


def header_last_line(src):
    # Return the 1-based line of the closing */ of the leading header block, or
    # 0 if the file does not open with a /* ... */ block. Lines up to and
    # including that one are the exempt license header.
    lines = src.splitlines()
    i = 0
    while i < len(lines) and not lines[i].strip():
        i += 1
    if i < len(lines) and lines[i].lstrip().startswith("/*"):
        for j in range(i, len(lines)):
            if "*/" in lines[j]:
                return j + 1
    return 0


def added_lines(diff_range, path):
    # New-side line numbers added by this diff. -U0 keeps the math simple: only
    # '+' lines appear in a hunk body, so each is the next new line.
    nums = set()
    new_line = 0
    out = git("diff", "--unified=0", "--diff-filter=ACMRT", diff_range, "--", path)
    for line in out.splitlines():
        if line.startswith("@@"):
            new_line = int(line.split("+", 1)[1].split(",", 1)[0].split(" ", 1)[0])
        elif line.startswith("+") and not line.startswith("+++"):
            nums.add(new_line)
            new_line += 1
    return nums


def introduces_block_comment(text):
    # True only if `text` opens a real /* comment - not a /* that sits inside a
    # string or char literal, and not one after a // line comment. C string
    # literals and // comments do not span lines, and the caller already skips
    # the license header, so this line-local scan is enough. It removes false
    # positives on AEL test inputs like ASSERT_HEX_EQ("$.a /* c */ > 1", ...)
    # and on /* sitting inside an existing // comment.
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            return False
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            return True
        if c in ('"', "'"):
            i += 1
            while i < n:
                if text[i] == "\\":
                    i += 2
                    continue
                if text[i] == c:
                    break
                i += 1
        i += 1
    return False


def main():
    base, head = parse_args(sys.argv[1:])
    diff_range = "%s...%s" % (base, head) if base else "origin/master...%s" % head
    print("Using diff range: %s" % diff_range)

    out = git("diff", "--name-only", "-z", "--diff-filter=ACMRT", diff_range)
    files = [p for p in out.split("\0") if p.endswith(SUFFIXES)]
    if not files:
        print("No C/C++ sources in diff range; nothing to check.")
        return 0

    print("Checking %d file(s) for newly introduced /* */ comments." % len(files))

    findings = []
    for path in files:
        src = git_optional("show", "%s:%s" % (head, path))
        if src is None:  # e.g. a rename's old path; nothing on the head side
            continue
        skip_through = header_last_line(src)
        added = added_lines(diff_range, path)
        for ln, text in enumerate(src.splitlines(), 1):
            if ln > skip_through and ln in added and introduces_block_comment(text):
                findings.append((path, ln))

    if not findings:
        print("OK: no new /* */ comments introduced.")
        return 0

    for path, ln in findings:
        print(
            "%s:%d: error: C-style /* */ comment introduced; use // "
            "(only the leading license header may be /* */)" % (path, ln),
            file=sys.stderr,
        )
    print(
        "\ncomment-style check failed: %d comment(s) across %d file(s)."
        % (len(findings), len({p for p, _ in findings})),
        file=sys.stderr,
    )
    print(
        "Convention: after the license header, write comments as // not /* */.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
