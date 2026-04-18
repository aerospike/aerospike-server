#!/usr/bin/env bash
# iwyu.bash
# Run include-what-you-use on CE and EE source files and apply fixes.

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
Usage: $(basename "$0") [OPTIONS] [file.c ...]

Run include-what-you-use (IWYU) on Aerospike source files and apply fixes.

Options:
    --dry-run   Show IWYU suggestions without applying changes
    --help      Show this help message

Arguments:
    file.c ...  Process only specified files (default: all source files)

Requirements:
    - iwyu_tool and fix_include (sudo apt install iwyu)
    - clang-format-diff (for formatting fixes)
    - compile_commands.json in parent directory
      Generate with: bear --output ../compile_commands.json -- make -j\$(nproc) +ee

Environment:
    EEREPO      Path to aerospike-server-enterprise repo (optional)

Examples:
    $(basename "$0")                    # Process all files in CE and EE repos
    $(basename "$0") --dry-run          # Show suggestions only
    $(basename "$0") as/src/base/cfg.c  # Process single file
EOF
    exit 0
}

DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
    --dry-run)
        DRY_RUN=true
        shift
        ;;
    --help | -h)
        usage
        ;;
    -*)
        log_error "Unknown option: $1"
        log_error "Use --help for usage information."
        exit 1
        ;;
    *)
        break
        ;;
    esac
done

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
CE_REPO="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"
WORKSPACE="$(dirname -- "$CE_REPO")"
MAPPING_FILE="$CE_REPO/.iwyu.imp"

# Check prerequisites.

if ! command -v iwyu_tool &>/dev/null; then
    log_error "Error: iwyu_tool not found. Install with: sudo apt install iwyu"
    exit 1
fi

if ! command -v fix_include &>/dev/null; then
    log_error "Error: fix_include not found. Install with: sudo apt install iwyu"
    exit 1
fi

if [[ ! -f "$WORKSPACE/compile_commands.json" ]]; then
    log_error "Error: compile_commands.json not found in $WORKSPACE"
    log_error "Generate with: bear --output ../compile_commands.json -- make -j\$(nproc) +ee"
    exit 1
fi

if [[ ! -f "$MAPPING_FILE" ]]; then
    log_error "Error: IWYU mapping file not found: $MAPPING_FILE"
    exit 1
fi

# Find clang-format (try versioned first)
CLANG_FORMAT=""
for cmd in clang-format-18 clang-format; do
    if command -v "$cmd" &>/dev/null; then
        CLANG_FORMAT="$cmd"
        break
    fi
done

if [[ -z "$CLANG_FORMAT" ]]; then
    log_error "Warning: clang-format not found. Include formatting may not match style."
fi

# Format just the include block of a file (includes + forward declarations).

format_include_block() {
    local file="$1"

    if [[ -z "$CLANG_FORMAT" || ! -f "$file" ]]; then
        return
    fi

    # Find the last line of the include/forward-declaration block.
    # Look for last #include or "struct foo;" line before real code starts.
    local last_include_line
    last_include_line=$(grep -n -E '^#include |^struct [a-z_]+;$' "$file" | tail -1 | cut -d: -f1)

    if [[ -z "$last_include_line" ]]; then
        return
    fi

    # Format from line 1 to end of include block
    "$CLANG_FORMAT" -i --lines=1:"$last_include_line" "$file" 2>/dev/null || true
}

# Files to exclude - IWYU incorrectly handles these (syscall macros, OpenSSL, etc.)
EXCLUDE_FILES=(
    "cf_mutex.c" # syscall() requires <unistd.h> which IWYU misses
    "fips_ee.c"  # OpenSSL provider functions
    "hardware.c" # Uses syscall() with __NR_* constants
    "tls_ee.c"   # OpenSSL headers vary by version (1.1 vs 3.0)
    "udf_cask.c" # IWYU flip-flops on <fcntl.h>
)

is_excluded() {
    local file="$1"
    local basename
    basename=$(basename "$file")

    for exclude in "${EXCLUDE_FILES[@]}"; do
        if [[ "$basename" == "$exclude" ]]; then
            return 0
        fi
    done

    return 1
}

# Filter IWYU output to remove known false positives.

filter_iwyu_output() {
    local output="$1"

    # Filter out suggestions that IWYU often gets wrong
    output=$(echo "$output" | grep -v -- '- #include "warnings.h"' || true)
    output=$(echo "$output" | grep -v -- '- #include "base/index.h"' || true)

    # Filter OpenSSL 3.0-specific headers (not available in OpenSSL 1.1.x)
    output=$(echo "$output" | grep -v -- '#include <openssl/types.h>' || true)
    output=$(echo "$output" | grep -v -- '#include <openssl/evperr.h>' || true)
    output=$(echo "$output" | grep -v -- '#include <openssl/sslerr.h>' || true)

    echo "$output"
}

# Run IWYU on a directory and apply fixes.

run_iwyu_dir() {
    local src_dir="$1"
    local repo_root="$2"
    local files=()

    if [[ ! -d "$src_dir" ]]; then
        return
    fi

    while IFS= read -r -d '' file; do
        if ! is_excluded "$file"; then
            files+=("$file")
        fi
    done < <(find "$src_dir" -name "*.c" ! -path "*/modules/*" -print0)

    if [[ ${#files[@]} -eq 0 ]]; then
        return
    fi

    log_info "  Processing ${#files[@]} files in $src_dir..."

    cd "$src_dir"

    # Run IWYU
    local iwyu_output
    iwyu_output=$(iwyu_tool -j0 -p "$WORKSPACE" "${files[@]}" -- -Xiwyu --mapping_file="$MAPPING_FILE" 2>&1 || true)

    # Filter output
    iwyu_output=$(filter_iwyu_output "$iwyu_output")

    if [[ "$DRY_RUN" == true ]]; then
        echo "$iwyu_output"
    else
        # Determine the source directory from first file (for fix_include)
        local first_file="${files[0]}"
        local fix_dir="$CE_REPO"

        if [[ "$first_file" == *"/as/"* ]]; then
            fix_dir="$CE_REPO/as/src"
        elif [[ "$first_file" == *"/cf/"* ]]; then
            fix_dir="$CE_REPO/cf/src"
        fi

        # Get list of modified files before fix_include
        cd "$repo_root"
        local before_files
        before_files=$(git diff --name-only 2>/dev/null || true)

        # Apply fixes with fix_include (from correct directory)
        cd "$fix_dir"
        echo "$iwyu_output" | fix_include --nocomments || true

        # Get list of modified files after fix_include
        cd "$repo_root"
        local after_files
        after_files=$(git diff --name-only 2>/dev/null || true)

        # Format include blocks of newly modified files
        if [[ -n "$CLANG_FORMAT" ]]; then
            local modified_file
            while IFS= read -r modified_file; do
                # Only format files that weren't modified before
                if [[ -n "$modified_file" ]] && ! echo "$before_files" | grep -qF "$modified_file"; then
                    format_include_block "$repo_root/$modified_file"
                fi
            done <<<"$after_files"
        fi
    fi
}

# Run IWYU on specific files.

run_iwyu_files() {
    local files=("$@")

    if [[ ${#files[@]} -eq 0 ]]; then
        return
    fi

    # Run IWYU
    local iwyu_output
    iwyu_output=$(iwyu_tool -j0 -p "$WORKSPACE" "${files[@]}" -- -Xiwyu --mapping_file="$MAPPING_FILE" 2>&1 || true)

    # Filter output
    iwyu_output=$(filter_iwyu_output "$iwyu_output")

    if [[ "$DRY_RUN" == true ]]; then
        echo "$iwyu_output"
    else
        # Determine repo and fix directory from first file
        local first_file="${files[0]}"
        local repo_root="$CE_REPO"
        local fix_dir="$CE_REPO"

        if [[ "$first_file" == *"$EEREPO"* ]] && [[ -n "${EEREPO:-}" ]]; then
            repo_root="$EEREPO"
            fix_dir="$EEREPO"
        fi

        # Determine the source directory (for fix_include relative paths)
        if [[ "$first_file" == *"/as/"* || "$first_file" == *"as/src"* ]]; then
            fix_dir="$repo_root/as/src"
        elif [[ "$first_file" == *"/cf/"* || "$first_file" == *"cf/src"* ]]; then
            fix_dir="$repo_root/cf/src"
        fi

        # Get list of modified files before fix_include
        cd "$repo_root"
        local before_files
        before_files=$(git diff --name-only 2>/dev/null || true)

        # Apply fixes with fix_include (from correct directory)
        cd "$fix_dir"
        echo "$iwyu_output" | fix_include --nocomments || true

        # Get list of modified files after fix_include
        cd "$repo_root"
        local after_files
        after_files=$(git diff --name-only 2>/dev/null || true)

        # Format include blocks of newly modified files
        if [[ -n "$CLANG_FORMAT" ]]; then
            local modified_file
            while IFS= read -r modified_file; do
                if [[ -n "$modified_file" ]] && ! echo "$before_files" | grep -qF "$modified_file"; then
                    format_include_block "$repo_root/$modified_file"
                fi
            done <<<"$after_files"
        fi
    fi
}

# Main.

if [[ $# -gt 0 ]]; then
    # Process specific files passed as arguments
    run_iwyu_files "$@"
else
    # Process all files in CE and EE repos
    log_info "CE repo: $CE_REPO"
    run_iwyu_dir "$CE_REPO/as/src" "$CE_REPO"
    run_iwyu_dir "$CE_REPO/cf/src" "$CE_REPO"

    if [[ -n "${EEREPO:-}" && -d "$EEREPO" ]]; then
        echo ""
        log_info "EE repo: $EEREPO"
        run_iwyu_dir "$EEREPO/as/src" "$EEREPO"
        run_iwyu_dir "$EEREPO/cf/src" "$EEREPO"
    else
        echo ""
        log_error "EE repo: not set (set EEREPO to include)"
    fi
fi
