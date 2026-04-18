#!/usr/bin/env bash
# Build and package edition(s) of aerospike-server. Invoke from repo root (aerospike-server).
#
# Usage: build_edition.bash [-a|--arch ARCH] [-e|--edition EDITION ...]
#
#   If no arguments are passed: build all editions (community, enterprise, fips) and all arch
#   (no arch filter — run on current runner; in a matrix this yields all editions × amd64 and arm64).
#
#   -a, --arch ARCH     Only run when current arch matches. ARCH: amd64, x86_64, arm64, or aarch64.
#   -e, --edition EDITION  Add EDITION to build list (use -e for each). EDITION: community, enterprise, fips.
#
# Requires: DISTRO (e.g. el8, el9, ubuntu24.04), HOST (e.g. ubuntu-24.04, ubuntu-24.04-arm).
#
# Examples:
#   build_edition.bash
#     No args: build all editions (community, enterprise, fips), no arch filter (all arch).
#
#   build_edition.bash -e community -e enterprise
#     Build only community and enterprise (skip fips).
#
#   build_edition.bash -a amd64
#     Build all default editions only when running on amd64/x86_64; skip on arm64.
#
#   build_edition.bash -a arm64 -e community -e enterprise
#     Build only community and enterprise, and only when running on arm64/aarch64.
#
#   build_edition.bash -a x86_64 -e fips
#     Build only fips, and only when running on x86_64 (fips is x86_64-only).

set -euo pipefail

DEFAULT_EDITIONS=(community enterprise fips)
ARCH_FILTER=
EDITIONS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
    -a | --arch)
        ARCH_FILTER="${2:?-a/--arch requires amd64|x86_64|arm64|aarch64}"
        shift 2
        ;;
    -e | --edition)
        case "${2:-}" in
        community | enterprise | fips)
            EDITIONS+=("$2")
            shift 2
            ;;
        *)
            echo "-e/--edition requires community, enterprise, or fips" >&2
            exit 1
            ;;
        esac
        ;;
    *)
        echo "Unknown option: $1" >&2
        echo "Usage: build_edition.bash [-a|--arch ARCH] [-e|--edition EDITION ...]" >&2
        exit 1
        ;;
    esac
done

if [[ ${#EDITIONS[@]} -eq 0 ]]; then
    EDITIONS=("${DEFAULT_EDITIONS[@]}")
fi

# Normalize --arch to amd64 or arm64 for comparison
case "$ARCH_FILTER" in
"") ;;
amd64 | x86_64) ARCH_FILTER=amd64 ;;
arm64 | aarch64) ARCH_FILTER=arm64 ;;
*)
    echo "Invalid --arch: $ARCH_FILTER (use amd64, x86_64, arm64, or aarch64)" >&2
    exit 1
    ;;
esac

current_arch=$(uname -m)
current_arch=${current_arch/x86_64/amd64}
current_arch=${current_arch/aarch64/arm64}
if [[ -n "$ARCH_FILTER" && "$current_arch" != "$ARCH_FILTER" ]]; then
    echo "Skipping build: current arch is $current_arch, filter is --arch $ARCH_FILTER"
    exit 0
fi

case "${DISTRO:?DISTRO required}" in
ubuntu* | debian*) pkg=deb ;;
*) pkg=rpm ;;
esac

CEREPO="$(pwd)"
export CEREPO
EEREPO="$(pwd)/modules/ee"
export EEREPO
FIPSREPO="$(pwd)/modules/fips"
export FIPSREPO

case "$DISTRO" in
el8)
    # shellcheck disable=SC1091
    source /opt/rh/gcc-toolset-12/enable
    ;;
el9)
    # shellcheck disable=SC1091
    source /opt/rh/gcc-toolset-14/enable
    ;;
*) ;;
esac

JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

do_edition() {
    local edition="$1"
    case "$edition" in
    community)
        make clean
        make -j"${JOBS}"
        make "${pkg}"
        ;;

    enterprise)
        make clean clean+ee
        make -j"${JOBS}" +ee
        make "${pkg}+ee"
        ;;

    fips)
        if [[ "${HOST:?HOST required}" == *arm* ]]; then
            echo "Skipping fips on arm64 (no arm64 FIPS builds)."
            return 0
        fi
        make clean clean+ee
        make -j"${JOBS}" +fips
        make "${pkg}+fips"
        ;;
    esac
}

for edition in "${EDITIONS[@]}"; do
    do_edition "$edition"
done
