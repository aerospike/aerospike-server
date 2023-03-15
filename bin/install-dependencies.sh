#!/usr/bin/env bash
set -Eeuo pipefail

OS=$(./build/os_version -long)

SUDO=""
if which sudo >/dev/null; then
	SUDO="$(which sudo)"
fi

echo "Installing server dependencies..."

case "$OS" in
'debian10' | 'debian11' | 'ubuntu20.04' | 'ubuntu22.04' )
    ${SUDO} apt-get update

    DEBIAN_FRONTEND=noninteractive ${SUDO} apt-get install -y --no-install-recommends \
        autoconf \
        automake \
        cmake \
        g++ \
        git \
        libtool \
        libssl-dev \
        make \
        zlib1g-dev
    ;;

# centos7 is the base image for rhel7
'centos7' | 'rhel8' | 'rhel9' )
    ${SUDO} yum install -y \
        autoconf \
        automake \
        cmake \
        gcc-c++ \
        git \
        libtool \
        openssl-devel \
        make \
        zlib-devel

    if [ "$OS" = "centos7" ]; then
        # Should install the release repo's before installing
        # the packages.
        ${SUDO} yum install centos-release-scl -y  # software collection repo
        ${SUDO} yum install epel-release -y

        ${SUDO} yum install -y \
            cmake3 \
            devtoolset-9-gcc \
            devtoolset-9-gcc-c++
    fi
    ;;
esac

if [ -n  "${EEREPO-}" ]; then
	"${EEREPO}"/bin/install-ee-dependencies.sh "${OS}"
fi
