#!/usr/bin/env bash
# Extract the package version string from the first .deb or .rpm found under a
# given directory.
#
# Usage: extract_version.bash <search-dir>
#
# Prints the version string to stdout and exits 0 on success.
# Exits 1 if no .deb or .rpm is found in the directory.
set -euo pipefail

search_dir="${1:?Usage: extract_version.bash <search-dir>}"

deb=$(find "$search_dir" -name '*.deb' -print -quit)
if [[ -n "$deb" ]]; then
    dpkg-deb -f "$deb" Version
    exit 0
fi

rpm=$(find "$search_dir" -name '*.rpm' -print -quit)
if [[ -n "$rpm" ]]; then
    rpm -qp --queryformat '%{VERSION}-%{RELEASE}' "$rpm"
    exit 0
fi

echo "ERROR: No .deb or .rpm found in '$search_dir'." >&2
exit 1
