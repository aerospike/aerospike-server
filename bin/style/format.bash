#!/usr/bin/env bash
# format.bash
# Recursively clang-formats CE and optional EE sources.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}$*${NC}"
}

log_error() {
    echo -e "${RED}$*${NC}" >&2
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
CE_REPO="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"

if ! command -v clang-format-18 &>/dev/null; then
    log_error "Error: clang-format-18 not found."
    exit 1
fi

CE_STYLE="file:$(realpath "$CE_REPO/.clang-format")"

CE_DIRS=(
    "$CE_REPO/as/src/"
    "$CE_REPO/as/include/"
    "$CE_REPO/cf/src/"
    "$CE_REPO/cf/include/"
)

EE_ROOT="${EEREPO:-}"
EE_DIRS=()

if [[ -n "$EE_ROOT" ]]; then
    EE_DIRS=(
        "$EE_ROOT/as/src/"
        "$EE_ROOT/as/include/"
        "$EE_ROOT/cf/src/"
        "$EE_ROOT/cf/include/"
        "$EE_ROOT/tests/unit/"
    )
fi

for d in "${CE_DIRS[@]}"; do
    if [[ -d "$d" ]]; then
        log_info "Formatting directory: $d"
        find "$d" -type f \( \
            -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o \
            -name '*.cxx' -o -name '*.h' -o -name '*.hpp' \) \
            -print0 | xargs -0 clang-format-18 -i
    else
        log_error "Warning: directory not found: $d"
    fi
done

for d in "${EE_DIRS[@]}"; do
    if [[ -d "$d" ]]; then
        log_info "Formatting directory: $d"
        find "$d" -type f \( \
            -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o \
            -name '*.cxx' -o -name '*.h' -o -name '*.hpp' \) \
            -print0 | xargs -0 clang-format-18 --style="$CE_STYLE" -i
    else
        log_error "Warning: directory not found: $d"
    fi
done
