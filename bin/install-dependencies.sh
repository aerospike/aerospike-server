#!/usr/bin/env bash
set -Eeuo pipefail

function usage() {
	echo
	echo "Usage: $0 [-d -r -h]"
	echo
	echo "-d: print the package update commands instead of running them (dry-run)"
	echo "-r: install the packages for runtime only (skip the dev packages)"
	echo "-h: print this help message"
}


function log_info() {
	echo -e "LOG-INFO\t\e[32m$1\e[0m" 1>&2
}


function log_warn() {
	echo -e "LOG-WARN\t\e[32m$1\e[0m" 1>&2
}


function parse_args() {
	RUNTIME_ONLY="false"
	DRY_RUN="false"

	while getopts "dhr" opt; do
		case "${opt}" in
		d )
			DRY_RUN="true"
			;;
		h )
			usage
			exit 0
			;;
		r )
			RUNTIME_ONLY="true"
			;;
		*)
			log_info "Unexpected option: ${opt}"
			usage
			exit 1
			;;
		esac
	done
}


function maybe() {
	if [ "$DRY_RUN" = "false" ]; then
		eval "$1";
	else
		echo "$1";
	fi
}


function main() {
	parse_args "$@"
	
	OS=${OS:="$(./build/os_version -long)"}

	SUDO=""
	if command -v sudo >/dev/null; then
		SUDO="$(command -v sudo)"
	fi

	log_info "Installing server dependencies, os=${OS}, runtime-only=${RUNTIME_ONLY}..."

	case "$OS" in
	'debian11' | 'debian12' | 'ubuntu20.04' | 'ubuntu22.04' | 'ubuntu24.04' )
		maybe "${SUDO} apt-get update"
		packages=(libssl-dev zlib1g-dev)  # Common packages (build + Runtime)
		# Add packages for build-only mode (i.e., runtime-only is not set)
		if [ "$RUNTIME_ONLY" = "false" ]; then
			# CE dependencies.
			packages+=(autoconf \
				automake \
				cmake \
				dpkg-dev \
				fakeroot \
				g++ \
				git \
				libtool \
				make \
				pkg-config)
			# EE dependencies.
			packages+=(libcurl4-openssl-dev
				   libldap2-dev)
		fi

		maybe "DEBIAN_FRONTEND=noninteractive ${SUDO} apt-get install -y --no-install-recommends ${packages[*]}"
		;;

	'amzn2023' | 'rhel8' | 'rhel9' )
		packages=(openssl-devel zlib-devel)  # Common packages (build + Runtime).

		# Add packages for build-only mode (i.e., runtime-only is not set)
		if [ "$RUNTIME_ONLY" = "false" ]; then
			# CE dependencies.
			packages+=(autoconf \
				automake \
				cmake \
				gcc-c++ \
				git \
				libtool \
				make \
				rpm-build)
			# EE dependencies.
			packages+=(libcurl-devel
				   openldap-devel)
		fi

		maybe "${SUDO} yum install -y ${packages[*]}"
		;;

	*)
		log_warn "Aerospike server (CE) currently does not support the distribution ${OS}."
		exit 1
		;;
	esac

	log_info "Finished."
}

main "$@"
