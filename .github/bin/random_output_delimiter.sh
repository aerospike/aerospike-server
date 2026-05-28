#!/usr/bin/env bash
# Print an unpredictable GITHUB_OUTPUT heredoc delimiter: <prefix>_<32 hex chars>
# Usage: random_output_delimiter.sh <prefix>
set -euo pipefail
prefix="${1:?usage: random_output_delimiter.sh <prefix>}"
printf '%s_%s\n' "$prefix" "$(openssl rand -hex 16)"
