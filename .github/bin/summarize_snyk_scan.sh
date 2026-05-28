#!/usr/bin/env bash
# Summarize pkg/docker/*.snyk logs for Slack. Writes multiline "summary" to GITHUB_OUTPUT.
# Invoked from build-sign-deploy.yaml and sign-build-deploy.yaml (keep logic in one place).
#
# Required env: GITHUB_OUTPUT
# Optional env: ARTIFACT_URL (Slack link line when upload-artifact produced a URL)

: "${GITHUB_OUTPUT:?}"

# Safe default so Slack always has a Snyk section even if this script dies early.
# MUST precede `set -euo pipefail`.
echo "summary=_Snyk scan unavailable_" >>"$GITHUB_OUTPUT"
set -euo pipefail
shopt -s nullglob
snyk_files=(pkg/docker/*.snyk)
if [[ ${#snyk_files[@]} -eq 0 ]]; then
    echo "::notice::No Snyk scan logs found (test.sh -s may not have produced output)"
    exit 0
fi
summary=""
clean_count=0
vuln_count=0
fail_count=0
for f in "${snyk_files[@]}"; do
    # Arch from basename: <product>-<semver>-<distro>-<arch>-<14-digit-ts>.snyk (strip ts, last field = arch).
    base=$(basename "$f" .snyk)
    no_ts="${base%-*}"
    arch_candidate="${no_ts##*-}"
    if [[ "$arch_candidate" =~ ^(amd64|arm64|aarch64|x86_64)$ ]]; then
        arch="$arch_candidate"
    else
        arch="unknown"
        echo "::notice::Snyk log ${base}.snyk: parsed arch '${arch_candidate}' not in allowlist (amd64|arm64|aarch64|x86_64); using unknown"
    fi
    # Extract image name from Snyk's "Testing <image>..." output line
    image_ref=$(grep -oP '(?<=Testing )\S+' "$f" 2>/dev/null | head -1 || true)
    if [[ -n "$image_ref" ]]; then
        image_name="${image_ref##*/}"  # drop registry/path prefix
        image_name="${image_name%%:*}" # drop :tag suffix
    else
        image_name="$no_ts"
    fi
    critical=$(grep -c 'Critical severity vulnerability found in' "$f" 2>/dev/null || true)
    high=$(grep -c 'High severity vulnerability found in' "$f" 2>/dev/null || true)
    critical=${critical:-0}
    high=${high:-0}
    # "Tested" appears in both clean and vulnerable Snyk output; its absence means the scan failed/errored.
    if grep -q 'Tested' "$f" 2>/dev/null; then
        if ((critical + high > 0)); then
            echo "::warning::Snyk ${image_name} (${arch}): high/critical vulnerabilities detected (${critical} critical, ${high} high)"
            summary="${summary}:rotating_light: *${image_name}* (${arch}): high/critical (${critical} critical, ${high} high)"$'\n'
            vuln_count=$((vuln_count + 1))
        else
            echo "::notice::Snyk ${image_name} (${arch}): clean"
            summary="${summary}:white_check_mark: *${image_name}* (${arch}): clean"$'\n'
            clean_count=$((clean_count + 1))
        fi
    else
        echo "::warning::Snyk scan failed or incomplete for ${image_name} (${arch})"
        summary="${summary}:grey_question: *${image_name}* (${arch}): scan failed"$'\n'
        fail_count=$((fail_count + 1))
    fi
done
echo "::notice::Snyk scan complete: ${clean_count} clean, ${vuln_count} vulnerable, ${fail_count} failed"
# Append artifact link if the upload step produced one (Slack mrkdwn format).
artifact_url="${ARTIFACT_URL:-}"
[[ -n "$artifact_url" ]] && summary="${summary}<${artifact_url}|View Snyk scan logs>"$'\n'
delimiter="$(bash "$(dirname "${BASH_SOURCE[0]}")/random_output_delimiter.sh" SUMMARY)"
{
    echo "summary<<${delimiter}"
    printf '%s' "$summary"
    echo
    echo "${delimiter}"
} >>"$GITHUB_OUTPUT"
