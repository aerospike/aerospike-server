#!/usr/bin/env bash
#
# Refuse a docker_repo_ref that is not on aerospike-server.docker's master.
# actions/checkout will fetch any reachable sha -- fork commits served through
# the parent repository's network included -- so a job that executes code from
# that checkout must not run history that never reached the repo's master.
#
# Required env:
#   DOCKER_REPO_DIR  checkout of aerospike/aerospike-server.docker
#   DOCKER_REPO_REF  the ref that checkout was made at (for the error message)

set -euo pipefail
: "${DOCKER_REPO_DIR:?}" "${DOCKER_REPO_REF:?}"

git -C "$DOCKER_REPO_DIR" fetch --quiet --filter=tree:0 origin master
if ! git -C "$DOCKER_REPO_DIR" merge-base --is-ancestor HEAD FETCH_HEAD; then
    echo "::error::docker_repo_ref '${DOCKER_REPO_REF}' is not on aerospike-server.docker master."
    exit 1
fi
