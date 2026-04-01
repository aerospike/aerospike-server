#!/usr/bin/env bash
# Clone a private GitHub repo, optionally checking out a specific branch, plus
# its submodules.  Assumes url.insteadOf is already configured for credential
# injection so the clone URL stays credential-free.
#
# Usage: checkout_private_repo.bash <org/repo> <dest-path> [ref]
#
# Arguments:
#   org/repo   - GitHub repository in owner/name format
#   dest-path  - Destination directory (relative to cwd)
#   ref        - Branch/tag/SHA to check out. Falls back to default branch if
#                the ref doesn't exist on the remote.
set -euo pipefail

repo="${1:?Usage: checkout_private_repo.bash <org/repo> <dest-path> [ref]}"
dest="${2:?Usage: checkout_private_repo.bash <org/repo> <dest-path> [ref]}"
ref="${3:-}"

git clone "https://github.com/${repo}.git" "${dest}"
cd "${dest}"

git fetch --prune origin +refs/heads/*:refs/remotes/origin/*

if [[ -n "${ref}" ]] && git show-ref --verify --quiet "refs/remotes/origin/${ref}"; then
    git checkout "${ref}"
    echo "Checked out branch '${ref}'"
else
    echo "Using default branch 'master'"
fi

git submodule sync --recursive
git submodule update --init --recursive

git config --global --add safe.directory "$(pwd)"
