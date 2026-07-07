#!/usr/bin/env bash
# Install build dependencies for aerospike-server 7.1.x
#
# Usage: install_deps.bash <distro>
#
#   distro: debian11, debian12, debian13, ubuntu20.04, ubuntu22.04, ubuntu24.04,
#           el8, el9, el10, amzn2023
#
# Debian/Ubuntu use distribution packages only (no OpenSSL/curl from source into /usr):
# installing OpenSSL under /usr breaks dpkg-shlibdeps for .deb builds, and pulling in
# libgtest-dev breaks Snappy's bundled googletest linkage on this release line.
set -xeuo pipefail

SUDO=
if [[ $(id -u) -ne 0 ]] && command -v sudo >/dev/null; then
	SUDO=sudo
fi

DEBIAN_COMMON_DEPS='libssl-dev zlib1g-dev autoconf automake cmake dpkg-dev fakeroot g++ git libtool make pkg-config libcurl4-openssl-dev libldap2-dev'
EL_COMMON_DEPS='openssl-devel zlib-devel autoconf automake make cmake gcc gcc-c++ git libtool glibc-devel rpm-build libcurl-devel openldap-devel'

# --- Per-distro install functions ---------------------------------------------------

install_deps_debian11() {
	install_debian_common
}

install_deps_debian12() {
	install_debian_common
}

install_deps_debian13() {
	install_debian_common
}

install_deps_ubuntu2004() {
	install_debian_common
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
}

install_deps_amzn2023() {
	$SUDO dnf install -y $EL_COMMON_DEPS gcc-plugin-devel
}

# --- Helper functions ---------------------------------------------------------------

install_debian_common() {
	$SUDO apt-get update
	$SUDO apt-get install -y --no-install-recommends $DEBIAN_COMMON_DEPS "$@"
	$SUDO apt-get install -y --no-install-recommends \
		"gcc-$(gcc -dumpversion | cut -d. -f1)-plugin-dev"
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
