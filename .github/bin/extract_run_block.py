#!/usr/bin/env python3
"""Print one workflow step's `run:` script to stdout.

Usage: extract_run_block.py <workflow.yaml> <step name>

The decision logic in refresh-docker-images.yaml and refresh-docker-image-version.yaml
lives in `run:` blocks -- which version is the tip, whether a published image is behind,
which mutable tags may move. None of it is reachable from a test while it is embedded in
YAML, and a `workflow_dispatch` is far too expensive a way to find out that a branch is
wrong. This lifts a named step's script out so a harness can run it with injected env.

Block scalars only (`run: |`), which is what every step under test uses; a folded or
single-line `run:` raises rather than being silently mis-indented. Deliberately does not
import yaml: this runs in CI on whatever the runner image ships, and a parse that only
needs "find the step, take the indented block" should not add a dependency for it.
"""

import sys


def extract(path, step_name):
    lines = open(path).read().split("\n")
    needle = "- name: " + step_name

    for i, line in enumerate(lines):
        if line.strip() != needle:
            continue
        step_indent = len(line) - len(line.lstrip())
        # Scan forward through this step's keys for `run:`; stop at the next step or
        # at anything dedented out of the step.
        for j in range(i + 1, len(lines)):
            cur = lines[j]
            if not cur.strip():
                continue
            indent = len(cur) - len(cur.lstrip())
            if indent <= step_indent and cur.strip() != needle:
                break
            if indent != step_indent + 2:
                continue
            if cur.strip() == "run: |":
                return dedent_block(lines, j + 1, step_indent + 2)
            if cur.strip().startswith("run:"):
                raise SystemExit(
                    "step %r has a non-block `run:`; only `run: |` is supported"
                    % step_name
                )
        raise SystemExit("step %r has no `run:`" % step_name)
    raise SystemExit("no step named %r in %s" % (step_name, path))


def dedent_block(lines, start, key_indent):
    body = []
    block_indent = None
    for line in lines[start:]:
        if not line.strip():
            body.append("")
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= key_indent:
            break
        if block_indent is None:
            block_indent = indent
        body.append(line[block_indent:])
    while body and not body[-1]:
        body.pop()
    return "\n".join(body) + "\n"


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    sys.stdout.write(extract(sys.argv[1], sys.argv[2]))
