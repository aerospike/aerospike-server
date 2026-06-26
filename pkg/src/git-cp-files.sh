#!/usr/bin/env bash

set -euo pipefail

SCRIPT=${BASH_SOURCE[0]}
SCRIPT_PATH=$(cd "$(dirname "${SCRIPT}")" && pwd)
SCRIPT_BASE=$(basename "${SCRIPT}")

SOURCE=${1}
TARGET=${2}
DEPTH=${3:-}

if [ -z "${SOURCE}" ]; then
  echo "ERROR: Missing SOURCE argument." >&2
  exit 1
fi
if [ -z "${TARGET}" ]; then
  echo "ERROR: Missing TARGET argument." >&2
  exit 1
fi
if [ ! -d "${SOURCE}" ]; then
  echo "ERROR: SOURCE not found: ${SOURCE}" >&2
  exit 1
fi

if [ -n "${DEPTH}" ]; then
    if [ "${DEPTH}" -eq 0 ]; then
        exit 0
    fi
    DEPTH=$((DEPTH - 1))
fi

mkdir -p "${TARGET}"

EXCLUDE_PATTERNS=(
    "modules/icu/icu4j/"
    "modules/icu/icu4c/installation/"
    "modules/icu/icu4c/source/test/"
    "modules/icu/icu4c/source/lib/"
    "modules/icu/vendor/"
)

should_exclude() {
    local filepath="$1"
    for pattern in "${EXCLUDE_PATTERNS[@]}"; do
        if [[ "${filepath}" == *"${pattern}"* ]]; then
            return 0
        fi
    done
    return 1
}

IFS=$'\n'
for file in $(cd "${SOURCE}" && git ls-files --abbrev); do
    if [ -f "${SOURCE}/${file}" ]; then
        if should_exclude "${TARGET}/${file}"; then
            continue
        fi
        dir=$(dirname "${file}")
        if [ ! -z "${dir}" ] && [ ! -d "${TARGET}/${dir}" ]; then
            mkdir -p "${TARGET}/${dir}"
        fi
        cp -a "${SOURCE}/${file}" "${TARGET}/${file}"
    fi
done

for module in $(cd "${SOURCE}" && git submodule status | awk '{print $2}'); do
    bash "${SCRIPT_PATH}/${SCRIPT_BASE}" "${SOURCE}/${module}" "${TARGET}/${module}" "${DEPTH}" || {
        echo "ERROR: recursive copy failed for module ${module}" >&2
        exit 1
    }
done
