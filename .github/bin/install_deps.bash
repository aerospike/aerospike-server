#!/usr/bin/env bash
# Install build dependencies for aerospike-server.
#
# Usage: install_deps.bash <distro>
#
#   distro: debian11, debian12, debian13, ubuntu20.04, ubuntu22.04, ubuntu24.04,
#           el8, el9, el10, amzn2023
#
# Modeled after aerospike-admin/.github/packaging/project/install_deps.sh
set -xeuo pipefail

OPENSSL_VERSION="3.0.19"

SUDO=
if [[ $(id -u) -ne 0 ]] && command -v sudo >/dev/null; then
    SUDO=sudo
fi

DEBIAN_COMMON_DEPS='libssl-dev zlib1g-dev autoconf automake cmake dpkg-dev fakeroot g++ git libtool make pkg-config libcurl4-openssl-dev libldap2-dev'
EL_COMMON_DEPS='openssl-devel zlib-devel autoconf automake make cmake gcc gcc-c++ git libtool glibc-devel rpm-build libcurl-devel openldap-devel'

# --- Per-distro install functions ---------------------------------------------------

install_deps_debian11() {
    install_debian_common wget perl
    build_openssl3
}

install_deps_debian12() {
    install_debian_common
}

install_deps_debian13() {
    install_debian_common
}

install_deps_ubuntu2004() {
    install_debian_common wget perl
    build_openssl3
}

install_deps_ubuntu2204() {
    install_debian_common
}

install_deps_ubuntu2404() {
    install_debian_common
}

install_deps_el8() {
    $SUDO dnf install -y --enablerepo=ubi-8-appstream-rpms \
        $EL_COMMON_DEPS \
        gcc-toolset-12 gcc-toolset-12-gcc-plugin-devel
}

install_deps_el9() {
    $SUDO dnf install -y --enablerepo=ubi-9-appstream-rpms \
        $EL_COMMON_DEPS \
        gcc-toolset-14 gcc-toolset-14-gcc-plugin-devel
}

install_deps_el10() {
    $SUDO dnf install -y $EL_COMMON_DEPS

    local arch
    arch=$(uname -m)
    $SUDO dnf -y --disablerepo='*' \
        "--repofrompath=cs10-baseos,https://mirror.stream.centos.org/10-stream/BaseOS/${arch}/os/" \
        "--repofrompath=cs10-appstream,https://mirror.stream.centos.org/10-stream/AppStream/${arch}/os/" \
        "--repofrompath=cs10-crb,https://mirror.stream.centos.org/10-stream/CRB/${arch}/os/" \
        --setopt=cs10-baseos.gpgcheck=0 \
        --setopt=cs10-appstream.gpgcheck=0 \
        --setopt=cs10-crb.gpgcheck=0 \
        install gcc-plugin-devel gmp-devel gmp-c++ mpfr-devel libmpc-devel
    $SUDO dnf -y clean all
    $SUDO rm -rf /var/cache/dnf
}

install_deps_amzn2023() {
    $SUDO dnf install -y $EL_COMMON_DEPS gcc-plugin-devel
}

# --- Helper functions ---------------------------------------------------------------

install_debian_common() {
    # Install common packages first (brings in gcc/g++), then gcc-plugin-dev
    # which needs gcc already present to determine the version.
    $SUDO apt-get update
    $SUDO apt-get install -y --no-install-recommends $DEBIAN_COMMON_DEPS "$@"
    $SUDO apt-get install -y --no-install-recommends \
        "gcc-$(gcc -dumpversion | cut -d. -f1)-plugin-dev"
}

build_openssl3() {
    # debian11 / ubuntu20.04 ship OpenSSL 1.1 (EOL Sep 2023).
    # Build OpenSSL 3 so the server links against libcrypto.so.3 instead of
    # the vulnerable libcrypto.so.1.1.

    local multiarch
    multiarch="$(uname -m)-linux-gnu"
    local src_dir
    src_dir="$(mktemp -d --tmpdir src_build.XXXXXX)"

    echo "Building OpenSSL ${OPENSSL_VERSION} from source (system OpenSSL 1.1 is EOL)."

    wget -qO "$src_dir/openssl.tar.gz" \
        "https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/openssl-${OPENSSL_VERSION}.tar.gz"
    tar xzf "$src_dir/openssl.tar.gz" -C "$src_dir"

    pushd "$src_dir/openssl-${OPENSSL_VERSION}" >/dev/null
    ./config --prefix=/usr --libdir="lib/${multiarch}" shared
    make -j"$(nproc)"
    $SUDO make install_sw
    popd >/dev/null

    $SUDO ldconfig

    # Remove stale multiarch OpenSSL 1.1 headers so the compiler doesn't pick
    # them up before the newly installed OpenSSL 3 headers.  GCC searches
    # /usr/include/<multiarch>/ before /usr/include/, and the old opensslconf.h
    # defines OPENSSL_API_COMPAT=0 which OpenSSL 3's macros.h rejects.
    if [[ -d "/usr/include/${multiarch}/openssl" ]]; then
        echo "Removing stale OpenSSL 1.1 multiarch headers from /usr/include/${multiarch}/openssl"
        $SUDO rm -rf "/usr/include/${multiarch}/openssl"
    fi

    rm -rf "$src_dir"
}

# --- Main ---------------------------------------------------------------------------

main() {
    if [[ $# -ne 1 ]]; then
        echo "Usage: install_deps.bash <distro>" >&2
        echo "  distro: debian11, debian12, debian13, ubuntu20.04, ubuntu22.04, ubuntu24.04, el8, el9, el10, amzn2023" >&2
        exit 1
    fi

    local distro="$1"
    export DEBIAN_FRONTEND=noninteractive

    case "$distro" in
    debian11) install_deps_debian11 ;;
    debian12) install_deps_debian12 ;;
    debian13) install_deps_debian13 ;;
    ubuntu20.04) install_deps_ubuntu2004 ;;
    ubuntu22.04) install_deps_ubuntu2204 ;;
    ubuntu24.04) install_deps_ubuntu2404 ;;
    el8) install_deps_el8 ;;
    el9) install_deps_el9 ;;
    el10) install_deps_el10 ;;
    amzn2023) install_deps_amzn2023 ;;
    *)
        echo "Unsupported distro: $distro" >&2
        exit 1
        ;;
    esac

    echo "Dependencies installed for $distro."
}

main "$@"
