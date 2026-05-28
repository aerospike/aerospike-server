#!/usr/bin/env bash
# Tag release workflow helper: tag + push one repo (duplicate refs already verified).
# Usage: tag_release_repo.sh <ce|enterprise|fips>
#
# Required env: GIT_USER_NAME, GIT_USER_EMAIL, BRANCH, MODE
# For release / pretag modes: RELEASE_TAG and/or NEXT_PRETAG as applicable
# enterprise|fips: workflow must run configure_git_github_app_token.sh before these steps
# ce: GITHUB_WORKSPACE, GITHUB_SHA
# enterprise|fips: RUNNER_TEMP

set -euo pipefail

REPO_KIND="${1:?usage: tag_release_repo.sh <ce|enterprise|fips>}"

case "$REPO_KIND" in
ce)
    cd "${GITHUB_WORKSPACE:?}"
    sha="${GITHUB_SHA:?}"
    ;;
enterprise)
    # Auth: workflow runs configure_git_github_app_token.sh (url.insteadOf) before this step.
    remote_clone="https://github.com/citrusleaf/aerospike-server-enterprise.git"
    dst="${RUNNER_TEMP:?}/aerospike-server-enterprise"
    rm -rf "$dst"
    git clone --depth 1 --branch "$BRANCH" --single-branch "$remote_clone" "$dst"
    cd "$dst"
    sha=$(git rev-parse HEAD)
    ;;
fips)
    remote_clone="https://github.com/citrusleaf/aerospike-server-fips.git"
    dst="${RUNNER_TEMP:?}/aerospike-server-fips"
    rm -rf "$dst"
    git clone --depth 1 --branch "$BRANCH" --single-branch "$remote_clone" "$dst"
    cd "$dst"
    sha=$(git rev-parse HEAD)
    ;;
*)
    echo "Invalid repo kind: ${REPO_KIND} (expected ce, enterprise, or fips)" >&2
    exit 1
    ;;
esac

git config user.name "$GIT_USER_NAME"
git config user.email "$GIT_USER_EMAIL"

case "$MODE" in
both) echo "Using MODE=${MODE} RELEASE_TAG=${RELEASE_TAG} NEXT_PRETAG=${NEXT_PRETAG} BRANCH=${BRANCH}" ;;
release-only) echo "Using MODE=${MODE} RELEASE_TAG=${RELEASE_TAG} BRANCH=${BRANCH}" ;;
pretag-only) echo "Using MODE=${MODE} NEXT_PRETAG=${NEXT_PRETAG} BRANCH=${BRANCH}" ;;
esac

if [[ "$MODE" == "both" || "$MODE" == "release-only" ]]; then
    git tag -a "$RELEASE_TAG" -m "Release ${RELEASE_TAG} ${sha} (triggered by ${GIT_USER_NAME})"
fi

if [[ "$MODE" == "both" || "$MODE" == "pretag-only" ]]; then
    git commit --allow-empty -m "Begin ${NEXT_PRETAG} development cycle (triggered by ${GIT_USER_NAME})"
    git tag -a "$NEXT_PRETAG" -m "Pre-release ${NEXT_PRETAG} (triggered by ${GIT_USER_NAME})"
fi

push_refs=("HEAD:refs/heads/${BRANCH}")
if [[ "$MODE" == "both" || "$MODE" == "release-only" ]]; then
    push_refs+=("refs/tags/${RELEASE_TAG}")
fi
if [[ "$MODE" == "both" || "$MODE" == "pretag-only" ]]; then
    push_refs+=("refs/tags/${NEXT_PRETAG}")
fi
git push origin "${push_refs[@]}"
