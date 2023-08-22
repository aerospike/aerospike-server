#!/usr/bin/env bash
set -Eeuo pipefail

function usage() {
	echo
	echo "Usage: $0 [-r | --runtime-only]" 1>&2
	echo
	echo "-r|--runtime-only: install the packages for runtime only (skip the dev packages)"
	echo
}


function install_script_dependencies() {
	# Needed for 'which' and 'getopt'.

	echo -e "\e[32mInstalling script dependencies, os=${OS}...\e[0m"

	case "$OS" in
	'debian10' | 'debian11' | 'debian12' | 'ubuntu20.04' | 'ubuntu22.04' )
		apt-get install -y --no-install-recommends util-linux debianutils
		;;
	# centos7 is the base image for rhel7
	'amzn2023' | 'centos7' | 'rhel8' | 'rhel9' )
		yum install -y util-linux which
		;;
	*)
		echo -e "\e[31mAerospike server (CE) currently does not support the distribution ${OS}.\e[0m"
		exit 1
	esac
}


function parse_args() {
	RUNTIME_ONLY="false"

	local parsed_args=$(getopt -a -n test -o rh --long runtime-only,help -- "$@")
	local valid_args=$?

	if [ "${valid_args}" != "0" ]; then
		usage
	fi

	eval set -- "${parsed_args}"
	while true; do
		case "$1" in
		-r | --runtime-only)
			RUNTIME_ONLY="true"
			shift
			;;
		-h | --help)
			usage
			exit 0
			;;
		--)
			shift
			break
			;;
		*)
			echo -e "\e[31mUnexpected option: $1\e[0m"
			usage
			exit 1
			;;
		esac
	done
}


function main() {
	OS=$(./build/os_version -long)
	install_script_dependencies
	parse_args "$@"

	SUDO=""
	if which sudo >/dev/null; then
		SUDO="$(which sudo)"
	fi

	echo -e "\e[32mInstalling server dependencies, os=${OS}, runtime-only=${RUNTIME_ONLY}...\e[0m"

	case "$OS" in
	'debian10' | 'debian11' | 'debian12' | 'ubuntu20.04' | 'ubuntu22.04' )
		${SUDO} apt-get update
		packages=(libssl-dev zlib1g-dev)  # # Common packages (build + Runtime)
		# Add packages for build-only mode (i.e., runtime-only is not set)
		if [ "$RUNTIME_ONLY" = "false" ]; then
			packages+=(autoconf \
				automake \
				cmake \
				dpkg-dev \
				fakeroot \
				g++ \
				git \
				libtool \
				make)
		fi

		DEBIAN_FRONTEND=noninteractive ${SUDO} apt-get install -y --no-install-recommends "${packages[@]}"
		;;

	# centos7 is the base image for rhel7
	'amzn2023' | 'centos7' | 'rhel8' | 'rhel9' )
		packages=(openssl-devel zlib-devel)  # Common packages (build + Runtime).

		# Add packages for build-only mode (i.e., runtime-only is not set)
		if [ "$RUNTIME_ONLY" = "false" ]; then
			packages+=(autoconf \
				automake \
				cmake \
				gcc-c++ \
				git \
				libtool \
				make \
				rpm-build)
		fi

		${SUDO} yum install -y "${packages[@]}"

		# Special provision for centos7.
		if [ "$RUNTIME_ONLY" = "false" ] && [ "$OS" = "centos7" ]; then
			# Should install the release repo's before installing
			# the packages.
			${SUDO} yum install -y centos-release-scl  # software collection repo
			${SUDO} yum install -y epel-release

			${SUDO} yum install -y \
				cmake3 \
				devtoolset-9-gcc \
				devtoolset-9-gcc-c++
		fi
		;;
	*)
		echo -e "\e[31mAerospike server (CE) currently does not support the distribution ${OS}.\e[0m"
		;;
	esac

	if [ -n  "${EEREPO-}" ]; then
		opt_flags=()
		[[ ${RUNTIME_ONLY} == true ]] && opt_flags+=(--runtime-only)

		# 'set +u' as a  workaround for a centos7 bug (unbound variable);
		# "${opt_flags[@]:+${opt_flags[@]}}" could be another option.
		[[ $OS = "centos7" ]] && set +u
		"${EEREPO}"/bin/install-ee-dependencies.sh --os="${OS}" "${opt_flags[@]}"
		[[ $OS = "centos7" ]] && set -u
	fi

	echo -e "\e[32mFinished.\e[0m"
}

main "$@"
