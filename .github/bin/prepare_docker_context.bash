#!/usr/bin/env bash
# Generate one self-contained Docker build context per edition, ready to be
# handed to shared-workflows' reusable_docker-build-deploy.yaml via its
# gh-context-artifacts-json input.
#
# docker-build.sh -g emits Dockerfiles whose COPY sources sit at the build
# context root, but the reusable workflow checks out THIS repo as the context
# and unpacks the named artifact at <context>/<CTX_DEST>. Every COPY source
# therefore has to be re-anchored under CTX_DEST.
#
# Usage: prepare_docker_context.bash
#
# Required env:
#   PACKAGE_VERSION  version argument for docker-build.sh (e.g. 8.1.2.4)
#   ARTIFACTS_DIR    directory holding the signed .deb packages
#   DOCKER_REPO_DIR  checkout of aerospike/aerospike-server.docker
#   OUT_DIR          destination for the per-edition context trees
#   EDITIONS         space-separated editions (e.g. "community enterprise federal")
#   DISTRO_FILTER    docker-build.sh -d filter (e.g. ubuntu)
#   CTX_DEST         path under the build context root the artifact unpacks at
#
# Optional env:
#   ASADM_SOURCE     Where the standalone aerospike-asadm package comes from.
#                    Unset (the default) inherits docker-build.sh's own default,
#                    the JFrog repo matching the package format. Set to "none"
#                    to build without asadm, or to a JFrog repo URL, a direct
#                    .deb/.rpm URL, a local directory, or a local package.
#                    Unless it is "none", the resolved package must actually
#                    materialize in the generated Dockerfile — see below.
set -euo pipefail

: "${PACKAGE_VERSION:?}"
: "${ARTIFACTS_DIR:?}"
: "${DOCKER_REPO_DIR:?}"
: "${OUT_DIR:?}"
: "${EDITIONS:?}"
: "${DISTRO_FILTER:?}"
: "${CTX_DEST:?}"

die() {
    echo "::error::prepare_docker_context: $*" >&2
    exit 1
}

artifacts_dir="$(cd "$ARTIFACTS_DIR" && pwd)"
out_dir="$(mkdir -p "$OUT_DIR" && cd "$OUT_DIR" && pwd)"

read -ra editions <<<"$EDITIONS"
[[ "${#editions[@]}" -gt 0 ]] || die "EDITIONS is empty"

cd "$DOCKER_REPO_DIR"

# -g is the only mode that stops after Dockerfile generation. A local -u path
# makes emit.sh stage the .deb next to the Dockerfile and emit a COPY for it,
# which is what turns the release directory into a standalone build context.
#
# asadm: by default inherit docker-build.sh's own -A default, which picks the
# JFrog repo matching the package format. A repo URL resolves the newest
# published asadm at generation time; a direct package URL pins an exact
# version. Whatever is resolved is SHA256-pinned into the Dockerfile and
# checked at image build time. asadm is best-effort by design — if none is
# published for this distro/arch, docker-build.sh warns and the image is built
# without it rather than failing the release.
asadm_args=()
case "${ASADM_SOURCE:-}" in
"") ;;
none) asadm_args=(--no-asadm) ;;
http*) asadm_args=(-A "$ASADM_SOURCE") ;;
*)
    [[ -e "$ASADM_SOURCE" ]] || die "ASADM_SOURCE does not exist: ${ASADM_SOURCE}"
    asadm_args=(-A "$(cd "$(dirname "$ASADM_SOURCE")" && pwd)/$(basename "$ASADM_SOURCE")")
    ;;
esac

./docker-build.sh -g -e "${editions[@]}" -d "$DISTRO_FILTER" -u "$artifacts_dir" \
    ${asadm_args[@]+"${asadm_args[@]}"} "$PACKAGE_VERSION"

lineage="$(cut -d. -f1,2 <<<"$PACKAGE_VERSION")"

# The version every edition resolved, emitted as a step output so the build jobs
# can stamp it onto the image as a label -- the only record that travels with the
# manifest into the release bundle. "none" when asadm is deliberately excluded.
ctx_asadm_ver=""
[[ "${ASADM_SOURCE:-}" != none ]] || ctx_asadm_ver=none

for edition in "${editions[@]}"; do
    mapfile -t dirs < <(
        find "releases/${lineage}/${edition}" -maxdepth 1 -mindepth 1 -type d \
            -name "${DISTRO_FILTER}*" 2>/dev/null | sort
    )
    [[ "${#dirs[@]}" -eq 1 ]] || die \
        "expected exactly one '${DISTRO_FILTER}*' context under releases/${lineage}/${edition}, found ${#dirs[@]}: ${dirs[*]:-<none>}"
    src="${dirs[0]}"
    dockerfile="${src}/Dockerfile"
    [[ -f "$dockerfile" ]] || die "no Dockerfile generated at ${dockerfile}"

    compgen -G "${src}/*.deb" >/dev/null || die \
        "no .deb staged in ${src}; docker-build.sh did not resolve ${artifacts_dir} as a local package source"

    # The server package must be the .deb this run just signed, staged via COPY.
    # A remote serverUrl means docker-build.sh resolved some other build and the
    # image would not contain the artifact being released. asadm is exempt: it
    # ships on its own release train and may legitimately come from JFrog.
    remote_server="$(grep -nE "serverUrl='https?://" "$dockerfile" || true)"
    [[ -z "$remote_server" ]] || die \
        "remote server URL in ${dockerfile} — the image would not ship the signed artifact: ${remote_server}"

    # asadm resolution is warning-only in docker-build.sh, so an unpublished or
    # mistyped source would quietly produce an image without asadm. Anything but
    # a deliberate ASADM_SOURCE=none has to actually land.
    #
    # It has to land PER ARCH. The generated Dockerfile inlines install-native.sh,
    # which carries an independent asadmUrl in each of its amd64 and arm64
    # branches, and emit.sh drops either one on its own with a warning
    # ("No arm64 asadm at ... - the arm64 image will have none"). One grep for a
    # resolved URL therefore passes when only one arch resolved, and that arch's
    # image gets an administration shell while the other silently does not. A
    # direct -A .deb URL applies only to the arch its filename names, so the
    # obvious way to pin a version hits this every time.
    #
    # The arches to require are derived from the server packages docker-build.sh
    # staged for THIS edition, so federal (amd64 only) does not demand an arm64
    # asadm, and there is no second arch list here to drift from the platforms:
    # each docker-* job passes to the reusable workflow.
    #
    # The arches must also agree on the VERSION. docker-build.sh resolves each
    # arch independently (sort -V | tail -1 per arch), so an asadm release that
    # lands amd64 before arm64 yields one index whose halves carry different
    # asadm builds. Requiring equality turns that publish skew into a failed
    # run -- re-runnable once the second arch lands -- instead of a shipped
    # image nobody can describe in one sentence.
    if [[ "${ASADM_SOURCE:-}" != none ]]; then
        asadm_ver=""
        asadm_ver_arch=""
        for arch in amd64 arm64; do
            compgen -G "${src}/aerospike-server-*_${arch}.deb" >/dev/null || continue
            # asadm is published under the kernel arch spelling on some suites
            # (_aarch64.deb) while the server debs use the dpkg one (_arm64.deb).
            case "$arch" in
            amd64) alt=x86_64 ;;
            arm64) alt=aarch64 ;;
            esac

            # Whatever this arch actually resolved to: a SHA256-pinned remote URL
            # in the Dockerfile, or a package staged into the context.
            ref="$(sed -nE "s/.*asadmUrl='(https?:\/\/[^']*_(${arch}|${alt})\.deb)'.*/\1/p" "$dockerfile" | head -1)"
            [[ -n "$ref" ]] || ref="$(compgen -G "${src}/aerospike-asadm*_${arch}.deb" | head -1 || true)"
            [[ -n "$ref" ]] || ref="$(compgen -G "${src}/aerospike-asadm*_${alt}.deb" | head -1 || true)"
            [[ -n "$ref" ]] || ref="$(compgen -G "${src}/aerospike-asadm*_all.deb" | head -1 || true)"
            [[ -n "$ref" ]] || die "no ${arch} asadm resolved for ${edition} from '${ASADM_SOURCE:-<docker-build.sh default>}'; the ${arch} image would ship without it. Point ASADM_SOURCE at a source published for every arch this edition builds — a direct .deb URL covers only its own arch — or set it to 'none' to build without asadm deliberately."

            # aerospike-asadm_<version>_<arch>.deb -> <version>
            ver="$(basename "$ref")"
            ver="${ver#aerospike-asadm[-_]}"
            ver="${ver%_*}"

            if [[ -z "$asadm_ver" ]]; then
                asadm_ver="$ver"
                asadm_ver_arch="$arch"
            elif [[ "$ver" != "$asadm_ver" ]]; then
                die "asadm version differs across arches for ${edition}: ${asadm_ver_arch}=${asadm_ver}, ${arch}=${ver}. Each arch resolves independently, so this is usually a half-finished publish — re-run once both arches carry the same version, or pin ASADM_SOURCE at one that does."
            fi
        done
        if [[ -n "$asadm_ver" ]]; then
            echo "asadm for ${edition}: ${asadm_ver} (${DISTRO_FILTER})"
            if [[ -z "$ctx_asadm_ver" ]]; then
                ctx_asadm_ver="$asadm_ver"
            elif [[ "$asadm_ver" != "$ctx_asadm_ver" ]]; then
                die "asadm version differs across editions: ${ctx_asadm_ver} vs ${edition}=${asadm_ver}. One generation run resolves one source, so this should be impossible; the image label would name only one of them."
            fi
        fi
    fi

    # Refuse any COPY shape the rewrite below would silently mangle: --chown /
    # --from flags, absolute sources, or more than one source.
    #
    # The three checks below come first because the pattern check cannot see them.
    # grep and perl -p are both line-oriented, so a backslash continuation splits one
    # logical COPY across physical lines: `COPY a \` tokenizes as `a` + `\`, which
    # SATISFIES the pattern, and the rewrite then anchors `a` and leaves the sources on
    # the following lines untouched -- silently copying a context-root file of the same
    # name, which is exactly the mangling this guard exists to prevent. The JSON exec
    # form tokenizes as `["src",` + `"dst"]` and passes for the same reason. And
    # Dockerfile keywords are case-insensitive, so a lowercase `copy --from=` needs
    # nothing from the context and would build cleanly, never re-anchored.
    #
    # ADD is refused outright rather than re-anchored: it should never appear in a
    # generated context, and treating it as an error keeps the rewrite's input set
    # exactly the shape the pattern below describes.
    if grep -qE '^[Cc][Oo][Pp][Yy] .*\\[[:space:]]*$' "$dockerfile"; then
        die "line-continued COPY in ${dockerfile}; the rewrite is line-oriented and would leave its later sources un-anchored"
    fi
    if grep -qE '^[Cc][Oo][Pp][Yy][[:space:]]*\[' "$dockerfile"; then
        die "JSON-array COPY in ${dockerfile}; the rewrite is not JSON-aware"
    fi
    if grep -qiE '^ADD[[:space:]]' "$dockerfile"; then
        die "unexpected ADD in ${dockerfile}; only COPY is re-anchored into ${CTX_DEST}/"
    fi
    # The first grep is -i so any non-canonical spelling lands in "unsupported" and
    # dies; the second stays case-sensitive because the rewrite only handles '^COPY '.
    unsupported="$(grep -iE '^copy ' "$dockerfile" | grep -vE '^COPY [^ /-][^ ]* [^ ]+$' || true)"
    [[ -z "$unsupported" ]] || die "unsupported COPY form in ${dockerfile}: ${unsupported}"

    CTX_DEST="$CTX_DEST" perl -pi -e \
        's{^COPY (?![-/])(\S+) }{COPY $ENV{CTX_DEST}/$1 }' "$dockerfile"

    # upload-artifact zips without permissions, so entrypoint.sh arrives as 644
    # and tini cannot exec it. Restore the mode in the image rather than relying
    # on the artifact round-trip surviving it.
    COPY_LINE="COPY ${CTX_DEST}/entrypoint.sh /entrypoint.sh" perl -pi -e \
        's{^\Q$ENV{COPY_LINE}\E$}{$&\nRUN chmod 0755 /entrypoint.sh}' "$dockerfile"
    grep -qxF 'RUN chmod 0755 /entrypoint.sh' "$dockerfile" || die \
        "no '${CTX_DEST}/entrypoint.sh' COPY in ${dockerfile}; cannot restore its exec bit"

    # BuildKit prefers <dockerfile>.dockerignore over the context root's
    # .dockerignore. It is the only hook we have for keeping the rest of this
    # repo out of the context, since the reusable workflow builds with context=.
    cat >"${src}/Dockerfile.dockerignore" <<IGNORE
**
!${CTX_DEST}/**
IGNORE

    mkdir -p "${out_dir}/${edition}"
    cp -a "${src}/." "${out_dir}/${edition}/"
    echo "Prepared ${edition} context (${src} -> ${out_dir}/${edition}):"
    ls -l "${out_dir}/${edition}"
done

if [[ -n "${GITHUB_OUTPUT:-}" && -n "$ctx_asadm_ver" ]]; then
    echo "asadm_version=${ctx_asadm_ver}" >>"$GITHUB_OUTPUT"
fi
