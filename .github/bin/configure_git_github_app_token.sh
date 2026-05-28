#!/usr/bin/env bash
# Configure git url.insteadOf so HTTPS GitHub URLs authenticate via TOKEN without
# embedding the token in clone / ls-remote URL strings (avoids token leakage in git errors).
#
# Required env: TOKEN (GitHub App installation token, x-access-token)

set -euo pipefail

TOKEN="${TOKEN:?TOKEN is required}"

dest="https://x-access-token:${TOKEN}@github.com/"
git config --global "url.${dest}.insteadOf" "https://github.com/"
git config --global --add "url.${dest}.insteadOf" "git@github.com:"
git config --global --add "url.${dest}.insteadOf" "ssh://git@github.com/"
git config --global --add "url.${dest}.insteadOf" "git://github.com/"
