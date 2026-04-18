#!/usr/bin/env bash
# Install build dependencies for aerospike-server 8.1.x
#
# Usage: install_deps.bash <distro>
#
#   distro: debian11, debian12, debian13, ubuntu20.04, ubuntu22.04, ubuntu24.04,
#           el8, el9, el10, amzn2023
set -xeuo pipefail

OPENSSL_VERSION="3.0.19"
OPENSSL_SHA256="fa5a4143b8aae18be53ef2f3caf29a2e0747430b8bc74d32d88335b94ab63072"

CURL_VERSION="8.12.1"
CURL_SHA256="7b40ea64947e0b440716a4d7f0b7aa56230a5341c8377d7b609649d4aea8dbcf"

GTEST_COMMIT="52eb8108c5bdec04579160ae17225d66034bd723"  # v1.17.0

SUDO=
if [[ $(id -u) -ne 0 ]] && command -v sudo >/dev/null; then
    SUDO=sudo
fi

DEBIAN_COMMON_DEPS='libssl-dev zlib1g-dev autoconf automake cmake dpkg-dev fakeroot g++ git libtool make pkg-config libcurl4-openssl-dev libldap2-dev libgtest-dev'
EL_COMMON_DEPS='openssl-devel zlib-devel autoconf automake make cmake gcc gcc-c++ git libtool glibc-devel rpm-build libcurl-devel openldap-devel'

# --- Per-distro install functions ---------------------------------------------------

install_deps_debian11() {
    install_debian_common wget perl
    build_openssl3
    build_curl
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
    build_curl
}

install_deps_ubuntu2204() {
    install_debian_common wget perl
    build_curl
}

install_deps_ubuntu2404() {
    install_debian_common
}

install_deps_el8() {
    $SUDO dnf install -y --enablerepo=ubi-8-appstream-rpms \
        $EL_COMMON_DEPS \
        gcc-toolset-12 gcc-toolset-12-gcc-plugin-devel

    build_gtest
}

install_deps_el9() {
    $SUDO dnf install -y --enablerepo=ubi-9-appstream-rpms \
        $EL_COMMON_DEPS \
        gcc-toolset-14 gcc-toolset-14-gcc-plugin-devel

    build_gtest
}

install_deps_el10() {
    $SUDO dnf install -y $EL_COMMON_DEPS

    local arch
    arch=$(uname -m)

    # Import the CentOS Official GPG key before using CentOS Stream 10 mirrors.
    # Fingerprint: 99DB 70FA E1D7 CE22 7FB6 4882 05B5 55B3 8483 C65D
    $SUDO rpm --import https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official-SHA256

    $SUDO dnf -y --disablerepo='*' \
        "--repofrompath=cs10-baseos,https://mirror.stream.centos.org/10-stream/BaseOS/${arch}/os/" \
        "--repofrompath=cs10-appstream,https://mirror.stream.centos.org/10-stream/AppStream/${arch}/os/" \
        "--repofrompath=cs10-crb,https://mirror.stream.centos.org/10-stream/CRB/${arch}/os/" \
        "--setopt=cs10-baseos.gpgkey=https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official-SHA256" \
        "--setopt=cs10-appstream.gpgkey=https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official-SHA256" \
        "--setopt=cs10-crb.gpgkey=https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official-SHA256" \
        install gcc-plugin-devel gmp-devel gmp-c++ mpfr-devel libmpc-devel
    $SUDO dnf -y clean all
    $SUDO rm -rf /var/cache/dnf

    build_gtest
}

install_deps_amzn2023() {
    $SUDO dnf install -y $EL_COMMON_DEPS gcc-plugin-devel

    build_gtest
}

# --- Helper functions ---------------------------------------------------------------

install_debian_common() {
    $SUDO apt-get update
    $SUDO apt-get install -y --no-install-recommends $DEBIAN_COMMON_DEPS "$@"
    $SUDO apt-get install -y --no-install-recommends \
        "gcc-$(gcc -dumpversion | cut -d. -f1)-plugin-dev"
}

build_openssl3() {
    local multiarch
    multiarch="$(uname -m)-linux-gnu"
    local src_dir
    src_dir="$(mktemp -d --tmpdir src_build.XXXXXX)"

    echo "Building OpenSSL ${OPENSSL_VERSION} from source (system OpenSSL 1.1 is EOL)."

    wget -qO "$src_dir/openssl.tar.gz" \
        "https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/openssl-${OPENSSL_VERSION}.tar.gz"
    echo "${OPENSSL_SHA256}  $src_dir/openssl.tar.gz" | sha256sum -c -
    tar xzf "$src_dir/openssl.tar.gz" -C "$src_dir"

    pushd "$src_dir/openssl-${OPENSSL_VERSION}" >/dev/null
    ./config --prefix=/usr --libdir="lib/${multiarch}" shared
    make -j"$(nproc)"
    $SUDO make install_sw
    popd >/dev/null

    $SUDO ldconfig

    # Remove stale multiarch OpenSSL 1.1 headers so the compiler doesn't pick
    # them up before the newly installed OpenSSL 3 headers.
    if [[ -d "/usr/include/${multiarch}/openssl" ]]; then
        echo "Removing stale OpenSSL 1.1 multiarch headers from /usr/include/${multiarch}/openssl"
        $SUDO rm -rf "/usr/include/${multiarch}/openssl"
    fi

    rm -rf "$src_dir"
}

build_curl() {
    local multiarch
    multiarch="$(uname -m)-linux-gnu"
    local src_dir
    src_dir="$(mktemp -d --tmpdir curl_build.XXXXXX)"

    echo "Building curl ${CURL_VERSION} from source (system libcurl has high-sev CVEs)."

    wget -qO "$src_dir/curl.tar.gz" \
        "https://github.com/curl/curl/releases/download/curl-${CURL_VERSION//./_}/curl-${CURL_VERSION}.tar.gz"
    echo "${CURL_SHA256}  $src_dir/curl.tar.gz" | sha256sum -c -
    tar xzf "$src_dir/curl.tar.gz" -C "$src_dir"

    pushd "$src_dir/curl-${CURL_VERSION}" >/dev/null
    ./configure --prefix=/usr --libdir="/usr/lib/${multiarch}" \
        --with-openssl --enable-shared --disable-static --without-libpsl
    make -j"$(nproc)"
    $SUDO make install
    popd >/dev/null

    $SUDO ldconfig
    rm -rf "$src_dir"
}

build_gtest() {
    local gtest_exists=false
    for lib in /usr/local/lib/libgtest.a /usr/lib*/libgtest.a; do
        [[ -e "$lib" ]] && gtest_exists=true && break
    done

    if $gtest_exists; then
        echo "googletest already present; skipping source build."
        return
    fi

    local tmpdir
    tmpdir="$(mktemp -d --tmpdir gtest_build.XXXXXX)"

    git clone --no-checkout https://github.com/google/googletest.git "$tmpdir/googletest"
    cd "$tmpdir/googletest"
    git checkout "${GTEST_COMMIT}"
    actual_commit=$(git rev-parse HEAD)
    if [[ "$actual_commit" != "$GTEST_COMMIT" ]]; then
        echo "ERROR: googletest commit mismatch: expected ${GTEST_COMMIT}, got ${actual_commit}" >&2
        exit 1
    fi
    cd -
    cmake -S "$tmpdir/googletest" -B "$tmpdir/googletest/build"
    cmake --build "$tmpdir/googletest/build" -j"$(nproc)"
    $SUDO cmake --install "$tmpdir/googletest/build"

    rm -rf "$tmpdir"
}

# --- Main ---------------------------------------------------------------------------

main() {
    if [[ $# -ne 1 ]]; then
        echo "Usage: install_deps.bash <distro>" >&2
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
