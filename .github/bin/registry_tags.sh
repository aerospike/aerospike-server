#!/usr/bin/env bash
#
# Every tag an Artifactory Docker repo holds for one image, one per line, unfiltered and
# unsorted. The caller decides what shape of tag it is interested in.
#
# Artifactory's Docker layout is <repo>/<image>/<tag>/<file>, so the tag is the
# second-to-last path segment of anything found under it -- which is how this works
# without knowing which file names a tag.
#
# `*/*.json` and not `*/manifest.json`: a multi-arch tag is stored as list.manifest.json
# with the per-platform manifests in sibling sha256__<digest> folders, so matching only
# manifest.json would miss every multi-arch tag -- which is all of ours. Layer blobs have
# no .json extension, and the sha256__ folders are left for the caller's filter to drop.
#
# Exits non-zero, with the CLI's own message, if the listing could not be read. An empty
# result is NOT an error here: "this image has no tags" is a real answer, and only the
# caller knows whether it is a plausible one.
#
# Requires an authenticated `jf`.
#
# Usage: registry_tags.sh <repo> <image>

set -euo pipefail

repo=${1:?repo required (e.g. database-docker-prod-public-local)}
image=${2:?image required (e.g. aerospike-server)}

# stdout and stderr kept apart: `2>&1` would fold a jf warning into the JSON that jq
# then parses, and a warning is exactly the kind of thing that appears without warning.
err=$(mktemp)
trap 'rm -f "$err"' EXIT

out=$(jf rt search --include-dirs=false "${repo}/${image}/*/*.json" 2>"$err") || {
    echo "::error::could not list ${repo}/${image} in Artifactory: $(cat "$err")" >&2
    exit 1
}

# jq's own exit status, not a swallowed pipeline: a non-JSON answer must fail here
# rather than come back as an empty tag list that a caller reads as "no tags".
if ! paths=$(jq -r '.[].path' <<<"$out"); then
    echo "::error::${repo}/${image} listing was not the JSON array jf rt search returns." >&2
    exit 1
fi

awk -F/ 'NF > 1 { print $(NF - 1) }' <<<"$paths" | sort -u
