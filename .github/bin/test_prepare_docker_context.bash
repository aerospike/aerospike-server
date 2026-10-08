#!/usr/bin/env bash
#
# Tests for prepare_docker_context.bash's cache-bust injection.
#
# The injection is the only lever the refresh pipeline has over the BuildKit layer
# cache that reusable_docker-build-deploy hardcodes: get it wrong and the refresh
# republishes the vulnerable image under a fresh digest, with every downstream check
# still green. The POSITION assertions are the load-bearing part -- an ARG below the
# apt chain, or in a second stage, compiles and busts nothing.
#
# Only inject_cache_bust is exercised; the surrounding generation run needs
# docker-build.sh from another repo.

# The fixtures are Dockerfile text: single-quoted strings holding ${...} that must NOT
# expand (SC2016) and trailing backslashes that are line continuations in the FILE, not
# escapes in the shell (SC1003).
# shellcheck disable=SC2016,SC1003
set -uo pipefail

# shellcheck source=.github/bin/prepare_docker_context.bash
source "$(dirname "$0")/prepare_docker_context.bash"
# The sourced script's own `set -e` lands here; several cases assert a non-zero exit.
set +e

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0
pass() { echo "✓ $1"; }
fail() {
    echo "✗ $1"
    failures=$((failures + 1))
}

ARG_RUN='RUN : "${AEROSPIKE_REFRESH_EPOCH:?cache-bust build-arg not supplied}"'

# die() exits, so every case runs in its own subshell.
inject() {
    local file=$1
    (
        set -euo pipefail
        inject_cache_bust "$file"
    ) 2>&1
}

write_dockerfile() {
    local name=$1
    shift
    printf '%s\n' "$@" >"${WORK}/${name}"
    echo "${WORK}/${name}"
}

expect_ok() {
    local name=$1 file=$2 out rc
    out=$(inject "$file")
    rc=$?
    if [[ "$rc" -ne 0 ]]; then
        fail "${name}: rc=${rc} -- ${out}"
        return 1
    fi
    pass "$name"
}

expect_die() {
    local name=$1 file=$2 want=$3 out rc
    out=$(inject "$file")
    rc=$?
    if [[ "$rc" -eq 0 ]]; then
        fail "${name}: succeeded, expected a refusal"
    elif [[ "$out" != *"$want"* ]]; then
        fail "${name}: refusal did not mention '${want}': ${out}"
    else
        pass "$name"
    fi
}

line_of() { grep -nxF "$2" "$1" | head -1 | cut -d: -f1; }

# The shape the generated recipe actually has: the apt work is one RUN with the
# apt-get calls on backslash continuation lines, which is why the position check
# cannot probe for `^RUN .*apt-get`.
f=$(write_dockerfile ok.Dockerfile \
    'FROM ubuntu:24.04' \
    'LABEL org.opencontainers.image.vendor="Aerospike"' \
    '# hadolint ignore=DL3008,DL3015' \
    'RUN \' \
    '    apt-get update -y \' \
    '    && apt-get install -y --no-install-recommends ca-certificates procps curl \' \
    '    && rm -rf /var/lib/apt/lists/*' \
    'COPY docker-ctx/entrypoint.sh /entrypoint.sh' \
    'RUN chmod 0755 /entrypoint.sh' \
    'ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]')
if expect_ok "injects above the apt RUN" "$f"; then
    arg=$(line_of "$f" "$ARG_RUN")
    apt=$(grep -n '^RUN \\$' "$f" | head -1 | cut -d: -f1)
    from=$(grep -n '^FROM ' "$f" | head -1 | cut -d: -f1)
    if [[ "$arg" -lt "$apt" && "$arg" -gt "$from" ]]; then
        pass "  ARG sits between FROM and the apt RUN (FROM ${from}, ARG ${arg}, apt ${apt})"
    else
        fail "  ARG at ${arg} is not between FROM ${from} and apt RUN ${apt}"
    fi

    # The hadolint directive has to stay attached to the instruction it annotates:
    # re-emitted UNDER the injection, immediately above the apt RUN.
    hint=$(grep -n '^# hadolint ignore=' "$f" | head -1 | cut -d: -f1)
    if [[ "$hint" -eq $((apt - 1)) ]]; then
        pass "  the hadolint directive stays attached to the apt RUN"
    else
        fail "  hadolint directive at ${hint}, apt RUN at ${apt}: no longer adjacent"
    fi

    if grep -qxF 'ARG AEROSPIKE_REFRESH_EPOCH' "$f"; then
        pass "  the ARG is declared"
    else
        fail "  no ARG AEROSPIKE_REFRESH_EPOCH declared"
    fi

    # Declared without a default and read with :?, so a build that forgets the
    # build-arg fails loudly instead of caching the whole chain.
    if grep -qxF 'ARG AEROSPIKE_REFRESH_EPOCH=' "$f"; then
        fail "  the ARG carries a default; an unsupplied build-arg would not fail"
    else
        pass "  the ARG has no default"
    fi

    # Everything the recipe already had must survive verbatim.
    grep -qxF 'ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]' "$f" &&
        if grep -qxF 'COPY docker-ctx/entrypoint.sh /entrypoint.sh' "$f"; then
            pass "  the rest of the recipe is preserved"
        else
            fail "  the injection dropped existing instructions"
        fi
fi

# A recipe with nothing to bust: injecting into it would produce a Dockerfile whose
# only RUN is the no-op, and the refresh would silently achieve nothing.
f=$(write_dockerfile norun.Dockerfile \
    'FROM ubuntu:24.04' \
    'COPY docker-ctx/entrypoint.sh /entrypoint.sh' \
    'ENTRYPOINT ["/entrypoint.sh"]')
expect_die "a recipe with no RUN is refused" "$f" "cannot anchor the cache-bust ARG"

# ARG is stage-scoped in BuildKit, so one injection cannot cover a second stage's
# apt chain -- it compiles and busts nothing.
f=$(write_dockerfile multistage.Dockerfile \
    'FROM ubuntu:24.04 AS build' \
    'RUN apt-get update -y && apt-get install -y curl' \
    'FROM ubuntu:24.04' \
    'COPY --from=build /usr/bin/curl /usr/bin/curl' \
    'RUN apt-get update -y && apt-get install -y ca-certificates')
expect_die "a multi-stage recipe is refused" "$f" "multi-stage"

# The minimal recipe that still has something to bust. Also pins that the ARG lands
# below FROM: BuildKit rejects an ARG above the first FROM as part of the stage.
f=$(write_dockerfile minimal.Dockerfile \
    'FROM ubuntu:24.04' \
    'RUN apt-get update -y')
if expect_ok "a minimal recipe injects" "$f"; then
    arg=$(line_of "$f" "$ARG_RUN")
    if [[ "$arg" -eq 3 ]]; then
        pass "  ARG lands directly below FROM"
    else
        fail "  ARG at line ${arg}, expected 3"
    fi
fi

if [[ "$failures" -ne 0 ]]; then
    echo "${failures} test(s) failed"
    exit 1
fi
echo "All prepare_docker_context cache-bust tests passed."
