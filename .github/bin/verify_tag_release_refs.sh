#!/usr/bin/env bash
# Fail fast if any tag to be created already exists on any of the three origins.
# Run once after checkout + app token, before any local tag or push.
#
# Env: MODE, GITHUB_WORKSPACE (set by Actions). Requires configure_git_github_app_token.sh
# (url.insteadOf) for enterprise/fips ls-remote when those repos are private.
# Env when needed: RELEASE_TAG (both|release-only), NEXT_PRETAG (both|pretag-only)

set -euo pipefail

cd "${GITHUB_WORKSPACE:?}"

verify_absent() {
    local remote_spec="$1"
    local tag="$2"
    local label="$3"
    if git ls-remote --tags "$remote_spec" "refs/tags/${tag}" | grep -q .; then
        echo "ERROR: tag '${tag}' already exists on ${label}" >&2
        exit 1
    fi
}

# TOKEN + url.insteadOf must be configured (see configure_git_github_app_token.sh).
ee_url="https://github.com/citrusleaf/aerospike-server-enterprise.git"
fips_url="https://github.com/citrusleaf/aerospike-server-fips.git"

if [[ "$MODE" == "both" || "$MODE" == "release-only" ]]; then
    verify_absent origin "${RELEASE_TAG}" aerospike-server
    verify_absent "$ee_url" "${RELEASE_TAG}" aerospike-server-enterprise
    verify_absent "$fips_url" "${RELEASE_TAG}" aerospike-server-fips
fi

if [[ "$MODE" == "both" || "$MODE" == "pretag-only" ]]; then
    verify_absent origin "${NEXT_PRETAG}" aerospike-server
    verify_absent "$ee_url" "${NEXT_PRETAG}" aerospike-server-enterprise
    verify_absent "$fips_url" "${NEXT_PRETAG}" aerospike-server-fips
fi

echo "OK: tag(s) to create are absent on all three origins (MODE=${MODE})."
