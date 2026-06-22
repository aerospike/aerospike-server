#!/usr/bin/env bash
set -euo pipefail

# Source the package utils
# shellcheck source=.github/bin/package_utils.sh
source "$(dirname "$0")/package_utils.sh"

echo "Testing get_codename_for_deb..."
failures=0
# Test codename mapping
test_codename() {
    local input="$1"
    local expected="$2"
    local result
    if [ "$expected" = "ERROR" ]; then
        if ! result=$(get_codename_for_deb "$input" 2>&1); then
            echo "✓ $input -> failed as expected"
        else
            echo "✗ $input -> succeeded unexpectedly"
            failures=$((failures + 1))
        fi
    else
        result=$(get_codename_for_deb "$input")
        if [ "$result" = "$expected" ]; then
            echo "✓ $input -> $result"
        else
            echo "✗ $input -> $result (expected $expected)"
            failures=$((failures + 1))
        fi
    fi
}

test_codename "ubuntu20.04" "focal"
test_codename "ubuntu22.04" "jammy"
test_codename "ubuntu24.04" "noble"
test_codename "ubuntu26.04" "resolute"
test_codename "debian11" "bullseye"
test_codename "debian12" "bookworm"
test_codename "debian13" "trixie"
test_codename "unknown" "ERROR"

if [ "$failures" -ne 0 ]; then
    echo "$failures test(s) failed"
    exit 1
fi
