#!/usr/bin/env bash
#
# Every released w.x.y.z version, as the UNION of three independent records that a
# version shipped -- one per line, version-sorted ascending.
#
#   git ship tags     vw.x.y.z in this repo, created by a tag-release.yaml dispatch
#   PROD tags         w.x.y.z under <prod-repo>/<image> in Artifactory, written by
#                     the release bundle promotion
#   Docker Hub tags   w.x.y.z under <org>/<image>, written by the Hub copy
#
# No single one of them is authoritative, because nothing orders them, and the order has
# gone both ways: a version has reached Docker Hub weeks before its ship tag was created,
# and a version has had a ship tag while PROD did not carry it. A caller that reads one
# source and calls the newest thing it finds "the tip" will, inside such a window, move
# w.x / w.x.y / latest BACKWARDS onto an older release -- undoing a hotfix for everyone
# pulling by those tags, with every digest check still passing, because those check the
# write and never the decision.
#
# The union makes a release recorded ANYWHERE count. That can only over-estimate the
# tip, and an over-estimate means a mutable tag does not move -- the safe direction.
#
# Fails closed, loudly: a source that errors, or that comes back with no
# version-shaped tag at all, aborts the run. Falling back to the remaining sources
# would silently restore the single-source read this exists to prevent.
#
# Required env:
#   RV_HUB_ORG     Docker Hub org that carries the images (vars.AEROSPIKE_DOCKER_ORG_NAME)
#   RV_PROD_REPO   Artifactory docker repo holding the published images
# Optional env:
#   RV_GIT_DIR     repo to read ship tags from (default: cwd). Needs full tag history.
#   RV_IMAGES      images to list, space separated (default: the three editions)
#   RV_HUB_MAX_PAGES  pagination safety stop (default 50)
#
# Requires an authenticated `jf` for the PROD listing; the Docker Hub listing is
# anonymous.
#
# Usage: released_versions.sh
#        released_versions.sh | grep -E '^8\.1\.' | tail -1    # lineage tip

set -euo pipefail

# Four-segment releases only. A suffixed or timestamped tag -- 8.1.2.5-20260915181406,
# 8.2.0.0_20260922T013041Z, both of which Docker Hub carries -- is a build artifact,
# not a release coordinate, and version-sorts after the release it was cut from.
VERSION_RE='^[0-9]+(\.[0-9]+){3}$'

rv_die() {
    echo "::error::released_versions: $*" >&2
    exit 1
}

# Every fetcher prints RAW candidates and leaves filtering to rv_collect, so a parse
# failure is a non-zero exit rather than something `grep ... || true` swallows.
rv_git_versions() {
    local dir=${RV_GIT_DIR:-.} out
    out=$(git -C "$dir" tag --list 'v*') || return 1
    sed -n 's/^v//p' <<<"$out"
}

# The PROD listing, filtered to release coordinates. The layout handling lives in
# registry_tags.sh so this and publish-refreshed-tags.yaml read a repo the same way.
rv_prod_versions() {
    local image=$1
    bash "$(dirname "${BASH_SOURCE[0]}")/registry_tags.sh" "$RV_PROD_REPO" "$image"
}

# The registry API, not hub.docker.com's. Both are anonymous, but the web API caps a
# page at 100 tags and throttles to 403 part way through a repository this size --
# aerospike-server alone carries over a thousand tags, mostly timestamped builds -- so
# a listing that must be COMPLETE cannot be built on it. The registry API returns 1000
# per page under a pull token and paginates through the Link header's `last` cursor:
# two requests per image instead of eleven.
#
# Every page is read, because the listing is not version-ordered -- a release can sit
# on the last page behind a thousand build tags.
rv_hub_versions() {
    local image=$1 token url page=0 body link repo hdrs
    # mktemp with a RETURN trap, as image_packages.sh does: a fixed /tmp/...$$ path
    # leaks when rv_die fires mid-pagination.
    hdrs=$(mktemp)
    trap 'rm -f "$hdrs"' RETURN
    # Declared separately: `local a=$1 b=$a` declares both names before assigning, so
    # b would read an unset a and trip set -u.
    repo="${RV_HUB_ORG}/${image}"

    token=$(curl -fsS --max-time 30 --retry 3 --retry-delay 2 \
        "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${repo}:pull" |
        jq -r '.token // empty') ||
        rv_die "could not get a Docker Hub pull token for ${repo}"
    [[ -n "$token" ]] || rv_die "Docker Hub returned no pull token for ${repo}"

    url="https://registry-1.docker.io/v2/${repo}/tags/list?n=1000"
    while [[ -n "$url" ]]; do
        page=$((page + 1))
        [[ "$page" -le "${RV_HUB_MAX_PAGES:-50}" ]] ||
            rv_die "Docker Hub listing for ${repo} did not terminate within ${RV_HUB_MAX_PAGES:-50} pages"
        # Truncated explicitly rather than trusting -D to do it: a stale Link header
        # left from the previous page makes this loop follow the same cursor forever.
        : >"$hdrs"
        body=$(curl -fsS --max-time 60 --retry 3 --retry-delay 2 \
            -H "Authorization: Bearer ${token}" -D "$hdrs" "$url") ||
            rv_die "could not list Docker Hub tags for ${repo} (page ${page})"
        # A valid last page can carry "tags": null, which `.tags[]?` accepts; a page
        # that is not JSON at all must NOT be swallowed, so jq's own exit status is the
        # check. `(.tags // [])[]` keeps null working while a parse failure still fails.
        jq -r '(.tags // [])[]' <<<"$body" ||
            rv_die "Docker Hub returned unparseable JSON for ${repo} (page ${page})"

        # link: </v2/<repo>/tags/list?last=<tag>&n=1000>; rel="next"
        link=$(sed -n 's/^[Ll]ink: *<\([^>]*\)>.*rel="next".*/\1/p' "$hdrs" | head -1)
        url=""
        [[ -z "$link" ]] || url="https://registry-1.docker.io${link}"
    done
}

# Run one fetcher and return its version-shaped tags, or abort.
#
# Captured rather than read through `mapfile < <(fetcher)`: a process substitution runs
# in a subshell, so rv_die there exits only the subshell and the caller sees exit 0 with
# whatever the fetcher managed to print first. A Docker Hub failure on page 2 therefore
# used to return page 1's tags and look like success -- a release that appeared only on
# the lost page dropped out of the union, the tip came out low, and the caller could move
# w.x / latest BACKWARDS. The emptiness check below never caught it, because a partial
# answer is not an empty one.
#
# Empty is still a fault in its own right: all three sources carry every supported
# lineage, so "no version-shaped tag at all" means the listing was read wrong -- a renamed
# repo, a changed response shape, an auth downgrade that returns an empty page.
rv_collect() {
    local label=$1
    shift
    local raw filtered
    raw=$("$@") || rv_die "${label} could not be listed"
    filtered=$(grep -E "$VERSION_RE" <<<"$raw" || true)
    [[ -n "$filtered" ]] ||
        rv_die "${label} returned no w.x.y.z tag at all; refusing to resolve a tip from the remaining sources"
    printf '%s\n' "$filtered"
}

rv_main() {
    : "${RV_HUB_ORG:?RV_HUB_ORG is required (the Docker Hub org carrying the images)}"
    : "${RV_PROD_REPO:?RV_PROD_REPO is required (the Artifactory docker repo)}"

    local -a images
    read -ra images <<<"${RV_IMAGES:-aerospike-server aerospike-server-enterprise aerospike-server-federal}"
    [[ "${#images[@]}" -gt 0 ]] || rv_die "RV_IMAGES is empty"

    # `raw=$(rv_collect …) || exit 1`, never `mapfile <<<"$(rv_collect …)"`: a command
    # substitution is a subshell, so rv_die inside it exits only the substitution and
    # rv_main would carry on with whatever had been printed before the failure. The
    # explicit status check is what makes a mid-pagination abort reach the caller.
    local -a all=() got
    local raw
    raw=$(rv_collect "git ship tags in ${RV_GIT_DIR:-.}" rv_git_versions) || exit 1
    mapfile -t got <<<"$raw"
    all+=("${got[@]}")

    local image
    for image in "${images[@]}"; do
        raw=$(rv_collect "${RV_PROD_REPO}/${image}" rv_prod_versions "$image") || exit 1
        mapfile -t got <<<"$raw"
        all+=("${got[@]}")

        raw=$(rv_collect "docker.io/${RV_HUB_ORG}/${image}" rv_hub_versions "$image") || exit 1
        mapfile -t got <<<"$raw"
        all+=("${got[@]}")
    done

    printf '%s\n' "${all[@]}" | sort -u -V
}

# Sourced by test_released_versions.sh, which stubs the three fetchers.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    rv_main "$@"
fi
