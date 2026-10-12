#!/usr/bin/env bash
#
# Decide which mutable Docker tags (w.x.y.z, w.x.y, w.x, latest) a build may move in
# database-docker-dev-local and database-docker-test-local, and print them on one line,
# space separated. Empty output means "move nothing". The single definition of this
# policy for build-sign-deploy.yaml and sign-build-deploy.yaml.
#
# Ownership, so that no two lines ever write the same tag:
#   - hotfix/W.X.Y owns its w.x.y.z and w.x.y, plus w.x when it is the newest line of
#     its w.x family (ties with master go to the hotfix, see below). Never latest.
#   - master owns w.x.y.z, w.x.y, w.x and latest of its own line -- except while
#     hotfix/<master's w.x.y> exists. That branch carries the newer builds and the
#     releases of the line, so master yields all three to it and keeps only latest.
#     A version that is not master's own line (a hotfix release entered on the master
#     ref) moves nothing: it belongs to hotfix/<its w.x.y>.
#   - Any other hotfix/** name (hotfix/8.2.0.escrow) is not a release line: nothing.
#   - Any other ref: nothing under 'auto'; the master set under 'force'. 'force' never
#     changes what master or hotfix/** move.
#
# An unattended run (a nightly, or a push-triggered run) never writes w.x.y.z. Release
# and bump tags are placed by hand, sometimes days after later commits land, so such a
# run can still build the version that was just released and would repoint the
# released :w.x.y.z.
#
# Env:
#   REF_NAME         the ref being built (GITHUB_REF_NAME)
#   MODE             force, or anything else for auto (a push-triggered run has no input)
#   PACKAGE_VERSION  w.x.y.z-<n> for a release line; w.x.y.z-<descriptor> for a preview
#   UNATTENDED       true for a nightly or push-triggered run
#
# Needs a checkout of the built commit with full history, tags and every branch fetched
# under refs/remotes/origin/.
#
# Exits 1 with an ::error:: annotation, printing nothing on stdout, when the version
# names no w.x.y.z; when master or hotfix/** builds something other than a w.x.y.z-<n>
# release coordinate; or when master's w.x.y.z tag can't be resolved.
#
# Usage: mutable_docker_tags.sh

set -euo pipefail

: "${REF_NAME:?REF_NAME required}"
: "${PACKAGE_VERSION:?PACKAGE_VERSION required}"
mode=${MODE:-auto}
unattended=${UNATTENDED:-false}

# A trailing -<n> is the build number the packages carry (8.1.2.5-3, 8.1.3.0-88);
# anything else after w.x.y.z is a git descriptor (8.1.3.0-88-g0f294c425-1), which is
# what a preview build produces, or a legacy -start/-alpha anchor (8.1.1.3-start-5-g...).
if [[ "$PACKAGE_VERSION" =~ ^((([0-9]+\.[0-9]+)\.[0-9]+)\.[0-9]+)(-[0-9]+)?$ ]]; then
    release_coordinate=true
elif [[ "$PACKAGE_VERSION" =~ ^((([0-9]+\.[0-9]+)\.[0-9]+)\.[0-9]+)-.*$ ]]; then
    release_coordinate=false
else
    echo "::error::cannot derive w.x.y.z from package version '${PACKAGE_VERSION}'." >&2
    exit 1
fi
wxyz="${BASH_REMATCH[1]}"
wxy="${BASH_REMATCH[2]}"
wx="${BASH_REMATCH[3]}"

# Moving a shared tag on a guess is worse than stopping, so a release line refuses
# anything but a w.x.y.z-<n> coordinate whatever the mode.
require_release_coordinate() {
    if [[ "$release_coordinate" != "true" ]]; then
        echo "::error::'${PACKAGE_VERSION}' is not a w.x.y.z-<n> release coordinate (git descriptor or legacy -start/-alpha anchor)." >&2
        exit 1
    fi
}

# The w.x.y of a commit: its bare w.x.y.z tag (the anchor build/version uses) minus the
# last field.
line_of() {
    local anchor
    if ! anchor="$(git describe --first-parent --tags --abbrev=0 \
        --match '[0-9]*.[0-9]*.[0-9]*.[0-9]*' "$1")"; then
        echo "::error::cannot resolve master's w.x.y.z tag (at $1) to rank ${wx} against." >&2
        return 1
    fi
    echo "${anchor%.*}"
}

# The newest w.x.y in the w.x family among master's line ($1) and the hotfix/W.X.Y
# branches.
newest_line_of_family() {
    # A prefix, not a glob: for-each-ref's '*' does not cross '/'.
    {
        echo "$1"
        git for-each-ref --format='%(refname:lstrip=4)' refs/remotes/origin/hotfix/
    } | { grep -E "^${wx//./\\.}\.[0-9]+$" || true; } | sort -V | tail -n1
}

case "$REF_NAME" in
master)
    require_release_coordinate
    own_line="$(line_of HEAD)"
    if [[ "$wxy" != "$own_line" ]]; then
        echo "::notice::${PACKAGE_VERSION} is not master's ${own_line} line; leaving the mutable tags untouched (publish it from hotfix/${wxy})." >&2
        exit 0
    fi
    if git show-ref --verify --quiet "refs/remotes/origin/hotfix/${wxy}"; then
        echo "::notice::hotfix/${wxy} exists, so it owns ${wxyz}, ${wxy} and ${wx}; master moves only latest." >&2
        tags=(latest)
    else
        tags=("$wxyz" "$wxy" "$wx" latest)
    fi
    ;;
hotfix/*)
    if [[ "$REF_NAME" != "hotfix/${wxy}" ]]; then
        echo "::notice::'${REF_NAME}' is not the ${wxy} release line (hotfix/${wxy}); leaving the mutable tags untouched." >&2
        exit 0
    fi
    require_release_coordinate
    tags=("$wxyz" "$wxy")
    # Resolved here, not inside newest_line_of_family: errexit does not reach into a
    # command substitution, so a failure there would rank against nothing.
    master_line="$(line_of origin/master)"
    newest="$(newest_line_of_family "$master_line")"
    # A tie with master means master is still on this w.x.y, and master yields the line
    # to this branch (see the master arm), w.x included.
    if [[ "$newest" == "$wxy" ]]; then
        tags+=("$wx")
    else
        echo "Not taking ${wx}: newest ${wx} release line is '${newest:-none}', not ${wxy}." >&2
    fi
    ;;
*)
    if [[ "$mode" != "force" ]]; then
        echo "::notice::'${REF_NAME}' is not master or a hotfix release line; leaving the mutable tags untouched (re-dispatch with docker_mutable_tags=force to override)." >&2
        exit 0
    fi
    tags=("$wxyz" "$wxy" "$wx" latest)
    echo "::warning::docker_mutable_tags=force: pointing ${tags[*]} at ${PACKAGE_VERSION}, which is not a release-line build. latest returns with master's next run; ${wxy} and ${wx} with the next run of the line that owns them; ${wxyz} only with that line's next manual run, never a nightly. Repoint any tag no line owns by hand (or delete it, if this run created it) using the pre-move digests printed by the next step." >&2
    ;;
esac

if [[ "$unattended" == "true" ]]; then
    kept=()
    for tag in "${tags[@]}"; do
        [[ "$tag" == "$wxyz" ]] || kept+=("$tag")
    done
    tags=("${kept[@]}")
    echo "Unattended run: not moving ${wxyz}, which may already name a release." >&2
fi

echo "${tags[*]}"
