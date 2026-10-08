#!/usr/bin/env bash
#
# Report one image's OS packages, as a sorted "<name> <version>" set two images
# can be diffed on. Two questions, two modes:
#
#   pending <ref>    what the archive has that the image does not -- "is this
#                    image behind?". The refresh precheck's gate.
#   installed <ref>  what the image actually has -- "did this rebuild produce a
#                    different image than the one it replaces?". The post-build
#                    verify-refresh gate.
#
# They are deliberately different questions. The generated Dockerfile never runs
# apt-get upgrade: it installs ca-certificates, procps, curl and the aerospike
# .debs, and nothing else. So a rebuild can only change a package if apt
# re-resolves it as part of that install set, or if the base image itself moved.
# Base packages the recipe never touches stay exactly as the base ships them,
# and `pending` reports those too -- which makes `pending` the right signal for
# "worth rebuilding?" and the WRONG one for "did the rebuild achieve anything?".
# Comparing installed sets answers the second question directly: a cache replay
# reproduces the old set byte for byte.
#
# dist-upgrade, not upgrade, in `pending` mode: `apt-get upgrade` never installs
# a new package, so any upgrade that pulls one in is held back and emits no Inst
# line at all. That is the shape a security fix takes when it bumps an soname --
# libssl3 -> libssl3t64, the transition this refresh pipeline exists for --
# and counting Inst lines off `upgrade` would report it as "nothing pending".
# -s keeps it a simulation; nothing is installed.
#
# Exit 0  the image was read; the set (possibly empty) is on stdout.
# Exit 1  the image could not be read, or the output was not what this expects.
#         A caller gating on `pending` must treat that as "assume it needs a
#         refresh"; a caller gating on `installed` must treat it as "not
#         verified", never as "unchanged". A check that cannot tell must not
#         answer.
#
# Usage: image_packages.sh <pending|installed> <image-ref> [platform]

set -euo pipefail

# docker writes implicit-pull progress to stderr, and that progress names the image
# ref. Merging it into the captured stdout would put the ref itself into the "package
# set" a caller diffs -- two refs for one identical image would then always compare as
# different, which is exactly the answer `installed` must never give by accident. So
# stderr goes to a file, and is shown only when the run fails.
err=$(mktemp)
trap 'rm -f "$err"' EXIT

mode=${1:?mode required: pending|installed}
ref=${2:?image ref required}
platform=${3:-linux/amd64}

case "$mode" in
pending)
    # --error-on=any, because `apt-get update` exits 0 when it cannot reach the
    # archive -- fetch failures are warnings. `apt-get -s dist-upgrade` then reads
    # empty package lists and prints "0 upgraded, 0 newly installed, ...", which
    # satisfies the summary-line check below, so `pending` would report nothing and
    # a caller would mark the image current: the one answer this probe must never
    # give. Reproduced with `docker run --network none ubuntu:24.04` -- update rc=0,
    # then "0 upgraded"; with --error-on=any update exits 100 offline. The published
    # 7.1/7.2/8.1 images carry no extra apt sources, so it exits 0 with network.
    #
    # apt's own stderr is left on stderr (no 2>&1) so the outer 2>"$err" captures the
    # reason the failure branch prints.
    #
    # timeout, because a stalled archive connection hangs the container rather than
    # failing it, and a caller in a loop would otherwise spend its whole job budget
    # on one bad image.
    if ! out=$(timeout "${IMAGE_PACKAGES_TIMEOUT:-300}" \
        docker run --rm --user root --platform "$platform" \
        --entrypoint bash "$ref" -c \
        'apt-get update -qq --error-on=any >/dev/null && apt-get -s dist-upgrade 2>/dev/null' 2>"$err"); then
        echo "could not run apt inside ${ref}:" >&2
        cat "$err" >&2
        exit 1
    fi

    # `apt-get -s` always ends with "N upgraded, N newly installed, ...". Its
    # absence means this is not a simulation we can read -- a container that
    # exited early, an apt that failed quietly, an image without the archive
    # configured -- and an empty Inst count taken off that output would read as
    # "current".
    if ! grep -qE '^[0-9]+ upgraded, ' <<<"$out"; then
        echo "unrecognised apt output from ${ref}" >&2
        exit 1
    fi

    # Inst <name> [<installed>] (<candidate> <origin> [<arch>])   upgrade
    # Inst <name> (<candidate> <origin> [<arch>])                 new dependency
    # Both put the candidate version first inside the parentheses.
    awk '/^Inst / {
            rest = $0
            sub(/^[^(]*\(/, "", rest)
            split(rest, field, " ")
            print $2, field[1]
        }' <<<"$out" | sort
    ;;

installed)
    # No --user root: dpkg-query reads a world-readable database. No network either --
    # unlike `pending`, nothing here talks to an archive.
    # shellcheck disable=SC2016  # dpkg-query's own format syntax, not shell expansion
    if ! out=$(timeout "${IMAGE_PACKAGES_TIMEOUT:-300}" \
        docker run --rm --network none --platform "$platform" \
        --entrypoint dpkg-query "$ref" \
        -W -f='${Package} ${Version}\n' 2>"$err"); then
        echo "could not run dpkg-query inside ${ref}:" >&2
        cat "$err" >&2
        exit 1
    fi

    # A base image always carries hundreds of packages. A handful of lines means
    # we are reading an error, not a manifest, and an empty-ish set compared
    # against another empty-ish set would look like "unchanged".
    if [[ "$(grep -c . <<<"$out")" -lt 20 ]]; then
        echo "implausibly short dpkg-query output from ${ref}:" >&2
        echo "$out" >&2
        exit 1
    fi

    # Belt and braces on top of the stderr split above: anything that is not a
    # "<name> <version>" pair is not a package, and must not reach a set comparison.
    if grep -qvE '^[a-z0-9][a-z0-9+.:-]* [^ ]+$' <<<"$out"; then
        echo "unexpected non-package line in dpkg-query output from ${ref}:" >&2
        grep -vE '^[a-z0-9][a-z0-9+.:-]* [^ ]+$' <<<"$out" >&2
        exit 1
    fi

    sort <<<"$out"
    ;;

*)
    echo "unknown mode '${mode}': expected pending or installed" >&2
    exit 1
    ;;
esac
