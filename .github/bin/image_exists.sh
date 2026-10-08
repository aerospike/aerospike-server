#!/usr/bin/env bash
#
# Does this image ref exist in its registry? Three answers, three exit codes --
# because the two failure answers must never share a branch:
#
#   0  present.
#   2  absent. The registry answered, and the answer was "no such manifest".
#   1  could not tell: a 401 from an expired login, a 429, a 5xx, a timeout, a DNS
#      failure. The registry's own text is on stderr.
#
# `docker manifest inspect ... >/dev/null 2>&1` collapses 1 and 2 into one branch, and
# every caller here wants them apart. A caller that treats "could not tell" as "absent"
# turns a registry blip into an unverified publish; one that treats "absent" as "could
# not tell" fails a lineage's first refresh forever. Both have been live in this
# pipeline.
#
# The discrimination is deliberately narrow: only the two spellings a registry uses for
# a missing tag count as absent, and everything else -- including a message this script
# has not seen before -- lands in "could not tell", which every caller treats as the
# unsafe answer. Erring the other way would let a novel error string read as absence.
#
# Usage: image_exists.sh <image-ref>

set -uo pipefail

ref=${1:?image ref required}

# Bounded, because a stalled registry hangs the client rather than failing it, and the
# callers run this in a loop: the precheck does up to 12 of these plus 12 package probes
# inside one job timeout. rc 124 from timeout(1) is not one of the absent spellings, so a
# timeout lands in "could not tell", which is where it belongs.
#
# stdout is the manifest and is not wanted; stderr is the answer.
if err=$(timeout "${IMAGE_EXISTS_TIMEOUT:-60}" docker manifest inspect "$ref" 2>&1 >/dev/null); then
    exit 0
fi

# "no such manifest" is what both Artifactory and Docker Hub return for a tag that is
# not there; "manifest unknown" is the registry-v2 error code's own text, which some
# daemons surface instead.
if grep -qiE 'no such manifest|manifest unknown' <<<"$err"; then
    printf '%s\n' "$err" >&2
    exit 2
fi

printf '%s\n' "$err" >&2
exit 1
