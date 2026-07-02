#!/usr/bin/env python3
# test_format_ranges.py - unit tests for the pure range math in format_ranges.py.
# line_ranges() is a pure list->list function (no git, no filesystem), so these
# cases need no repo fixture.
#
# Run: python3 .github/bin/test_format_ranges.py   (exit 0 = pass)

import sys
from importlib import import_module

line_ranges = import_module("format_ranges").line_ranges

# (hunk headers, context, nlines, expected ranges)
CASES = [
    (["@@ -1,0 +1,3 @@"], 0, 100, [(1, 3)]),  # addition
    (["@@ -4 +4 @@"], 0, 100, [(4, 4)]),  # single line, ",c" absent
    (["@@ -5,2 +5,0 @@"], 0, 100, []),  # mid-file deletion: no range
    (["@@ -9,0 +0,0 @@"], 0, 100, []),  # leading-line deletion: no 1:0
    (["@@ -1,0 +5,1 @@"], 3, 5, [(2, 5)]),  # clamp hi to nlines, keep line 5
    (["@@ -1,0 +2,1 @@", "@@ -9,0 +4,1 @@"], 1, 50, [(1, 5)]),  # merge after pad
    (["@@ -1,0 +2,1 @@", "@@ -9,0 +9,1 @@"], 0, 50, [(2, 2), (9, 9)]),  # disjoint
    ([], 3, 100, []),  # no hunks
]


def main():
    failures = 0
    for headers, ctx, nlines, want in CASES:
        got = line_ranges(headers, ctx, nlines)
        ok = got == want
        mark = "OK  " if ok else "FAIL"
        print(
            "%s line_ranges(%r, ctx=%d, nlines=%d) -> %r"
            % (mark, headers, ctx, nlines, got)
        )
        if not ok:
            print("       expected %r" % (want,))
            failures += 1
    if failures:
        print("%d test(s) failed" % failures)
        return 1
    print("all %d test(s) passed" % len(CASES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
