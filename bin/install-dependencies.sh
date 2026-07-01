#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    cat <<'EOF'

Usage: install-deps.sh [-d] [-r] [-h]

  -d  Print the package commands instead of running them (dry-run)
  -r  Install runtime packages only (skip dev/build packages)
  -h  Print this help message

EOF
}

log_info() { printf 'LOG-INFO\t\033[32m%s\033[0m\n' "$*" >&2; }
log_warn() { printf 'LOG-WARN\t\033[33m%s\033[0m\n' "$*" >&2; }

DRY_RUN=false
RUNTIME_ONLY=false

parse_args() {
    while getopts ":drh" opt; do
        case "$opt" in
        d) DRY_RUN=true ;;
        r) RUNTIME_ONLY=true ;;
        h)
            usage
            exit 0
            ;;
        \?)
            log_warn "Unknown option: -$OPTARG"
            usage
            exit 1
            ;;
        esac
    done

    shift $((OPTIND - 1))

    if [[ $# -gt 0 ]]; then
        log_warn "Unexpected arguments: $*"
        usage
        exit 1
    fi
}

run() {
    # Print the command exactly as it would run, and run it unless DRY_RUN=true
    printf '+ ' >&2
    printf '%q ' "$@" >&2
    printf '\n' >&2
    $DRY_RUN || "$@"
}

have() { command -v "$1" >/dev/null 2>&1; }

main() {
    parse_args "$@"

    local os="${OS:-}"
    if [[ -z "$os" ]]; then
        os="$(./build/os_version -long)"
    fi

    local sudo=()
    if have sudo; then
        sudo=(sudo)
    fi

    log_info "Installing server dependencies (os=${os}, runtime-only=${RUNTIME_ONLY}, dry-run=${DRY_RUN})..."

    case "$os" in
    debian* | ubuntu*)
        run "${sudo[@]}" apt-get update

        local -a pkgs=(libssl-dev zlib1g-dev) # common (build + runtime)

        if ! $RUNTIME_ONLY; then
            # gcc-plugin.h is in gcc-<major>-plugin-dev; resolve the version apt maps 'gcc' to.
            local gcc_major
            # '|| true' keeps a non-zero apt-cache exit (cold cache, before
            # apt-get update) from tripping 'set -e' via pipefail, so the
            # fallback below is reachable instead of aborting the script.
            gcc_major=$(apt-cache show gcc 2>/dev/null |
                awk '/^Version:/{v=$2; sub(/^[0-9]+:/, "", v); sub(/\..*/, "", v); print v; exit}') || true
            : "${gcc_major:=13}" # fallback when apt-cache is unavailable or the cache is cold

            pkgs+=(
                autoconf automake cmake dpkg-dev fakeroot g++ git libtool make pkg-config
                libcurl4-openssl-dev libldap2-dev libgtest-dev
                "gcc-${gcc_major}-plugin-dev"
            )
        fi

        run env DEBIAN_FRONTEND=noninteractive \
            "${sudo[@]}" apt-get install -y --no-install-recommends "${pkgs[@]}"
        ;;

    amzn2023 | rhel* | el9* | centos* | rocky* | almalinux*)
        # Prefer dnf if present; fall back to yum
        local pm="yum"
        have dnf && pm="dnf"

        local -a pkgs=(openssl-devel zlib-devel) # common (build + runtime)

        if ! $RUNTIME_ONLY; then
            pkgs+=(
                autoconf automake make cmake gcc gcc-c++ git libtool glibc-devel rpm-build
                libcurl-devel openldap-devel gcc-plugin-devel
            )
        fi

        run "${sudo[@]}" "$pm" install -y "${pkgs[@]}"

        if ! $RUNTIME_ONLY; then
            # Build/install googletest only if it isn't already available.
            gtest_exists=false
            for lib in /usr/local/lib/libgtest.a /usr/lib*/libgtest.a; do
                [[ -e "$lib" ]] && gtest_exists=true && break
            done

            if ! $gtest_exists; then
                # Use predictable path for dry-run, actual temp dir otherwise
                if $DRY_RUN; then
                    TMPDIR_GTEST="/tmp/gtest_build_dryrun"
                else
                    # Intentionally not using a local variable.
                    TMPDIR_GTEST="$(mktemp -d --tmpdir gtest_build.XXXXXX)"

                    # Set up cleanup trap (only in real run, not dry-run)
                    cleanup_tmpdir() {
                        [[ -n "${TMPDIR_GTEST:-}" && -d "${TMPDIR_GTEST}" ]] && rm -rf "${TMPDIR_GTEST}"
                    }
                    trap cleanup_tmpdir EXIT
                fi

                local gtest_src="${TMPDIR_GTEST}/googletest"
                local gtest_build="${gtest_src}/build"

                run git clone --depth 1 https://github.com/google/googletest.git "$TMPDIR_GTEST/googletest"
                run cmake -S "$gtest_src" -B "$gtest_build"
                run cmake --build "$gtest_build" -j"$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)"
                run "${sudo[@]}" cmake --install "$gtest_build"
            else
                log_info "googletest already present; skipping source build."
            fi
        fi
        ;;

    *)
        log_warn "Unsupported distribution: ${os}"
        exit 1
        ;;
    esac

    log_info "Finished."
}

main "$@"
