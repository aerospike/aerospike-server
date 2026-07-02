#!/usr/bin/env python3
# format_ranges.py - one job: read `git diff -U0` on stdin, print the
# clang-format --lines=A:B args for the new-side changed lines (padded by
# CONTEXT, clamped to NLINES, overlapping/adjacent ranges merged).
#
# Args: CONTEXT NLINES.  Used by check_clang_format_changes.bash.
#
# Example:
#   printf '@@ -1,0 +5,2 @@\n' | format_ranges.py 3 100   ->  --lines=2:9

import sys


def line_ranges(hunk_headers, context, nlines):
    """Pure: new-side line ranges, padded by `context`, clamped to [1, nlines],
    overlapping/adjacent ranges merged. Empty if the file has only deletions."""
    spans = []
    for h in hunk_headers:
        plus = h.split("+", 1)[1].split(" ", 1)[0]  # "@@ -a,b +new,c @@" -> "new,c"
        new, _, count = plus.partition(",")
        new = int(new)
        c = int(count) if count else 1  # ",c" absent means one line
        if c == 0:  # pure deletion: no new-side lines
            continue
        lo = max(1, new - context)
        hi = min(nlines, new + c - 1 + context)
        if hi >= lo:  # skip anything clamped away
            spans.append((lo, hi))
    spans.sort()
    merged = []
    for lo, hi in spans:
        if merged and lo <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def main():
    context, nlines = int(sys.argv[1]), int(sys.argv[2])
    headers = [ln for ln in sys.stdin if ln.startswith("@@")]
    for lo, hi in line_ranges(headers, context, nlines):
        print("--lines=%d:%d" % (lo, hi))


if __name__ == "__main__":
    main()
