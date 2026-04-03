#!/usr/bin/env bash
# style.bash
# Orchestrates IWYU + formatting pipeline.

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

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Run style pipeline for CE/EE sources.

Options:
    --without-iwyu   Skip IWYU and run clang-format only
    --help, -h       Show this help message
EOF
    exit 0
}

WITH_IWYU=true

while [[ $# -gt 0 ]]; do
    case "$1" in
    --without-iwyu)
        WITH_IWYU=false
        shift
        ;;
    --help | -h)
        usage
        ;;
    *)
        log_error "Unknown option: $1"
        log_error "Use --help for usage information."
        exit 1
        ;;
    esac
done

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
STYLE_DIR="$SCRIPT_DIR/style"

if [[ "$WITH_IWYU" == true ]]; then
    log_info "Running IWYU..."
    "$STYLE_DIR/iwyu.bash"
fi

"$STYLE_DIR/format.bash"
