#!/usr/bin/env bash

CWD=$(pwd)
SCRIPT=${BASH_SOURCE[0]}
SCRIPT_PATH=$( cd "$( dirname "${SCRIPT}" )" && pwd )
SCRIPT_BASE=$( basename "${SCRIPT}" )

SOURCE=${1}
TARGET=${2}
DEPTH=${3}

if [ -z "${SOURCE}" ]; then
  echo "ERROR: Missing SOURCE argument." >&2
  exit 1
fi
if [ -z "${TARGET}" ]; then
  echo "ERROR: Missing TARGET argument." >&2
  exit 1
fi
if [ ! -d ${SOURCE} ]; then
  echo "ERROR: SOURCE not found: ${SOURCE}" >&2
  exit 1
fi

if [ -n "${DEPTH}" ]; then
	if [ ${DEPTH} -eq 0 ]; then
		exit 0
	fi
	DEPTH=$((DEPTH - 1))
fi

mkdir -p ${TARGET}

IFS=$'\n'
for file in $(cd ${SOURCE} && git ls-files --abbrev); do
  if [ -f ${SOURCE}/${file} ]; then
    dir=$(dirname ${file})
    if [ ! -z "${dir}" ] && [ ! -d ${TARGET}/${dir} ]; then
      mkdir -p "${TARGET}/${dir}"
    fi
    cp -a "${SOURCE}/${file}" "${TARGET}/${file}"
  fi
done

for module in $(cd ${SOURCE} && git submodule status | awk '{print $2}'); do
  bash ${SCRIPT_PATH}/${SCRIPT_BASE} ${SOURCE}/${module} ${TARGET}/${module} ${DEPTH}
done
