#!/usr/bin/env bash
#
# Download artifacts from S3 with retry logic
#
# Required environment variables:
#   GH_EVENT   - GitHub event name (workflow_dispatch or push)
#   S3_PATH    - Full S3 path to artifacts
#   S3_BUCKET  - S3 bucket name
#   VERSION    - Build version
#   COUNT      - Expected artifact count (for push events)
#

set -euo pipefail

echo "Starting artifact download attempt..."
mkdir -p build-artifacts

# List S3 once (key is last field so we handle human-readable size e.g. "2.9 MiB")
aws s3 ls "$S3_PATH" --recursive >/tmp/s3-listing.txt || true
grep -E '\.(deb|rpm)$' /tmp/s3-listing.txt | awk '{print $NF}' | grep -v '^[[:space:]]*$' >/tmp/artifact-list.txt
completed=$(wc -l </tmp/artifact-list.txt | tr -d ' ')
completed=$((completed + 0))

# Determine expected artifact count
if [ "$GH_EVENT" = "workflow_dispatch" ]; then
    expected=$completed
    if [ "$expected" -le 0 ]; then
        echo "ERROR: No artifacts found for version ${VERSION}" >&2
        exit 1
    fi
else
    expected=${COUNT:-0}
fi
if [ "$expected" -le 0 ]; then
    echo "ERROR: Expected artifact count is $expected" >&2
    exit 1
fi

echo "Expecting $expected artifacts"
echo "Have $completed / $expected artifacts on S3"

if [ "$completed" -lt "$expected" ]; then
    echo "Completed $completed / $expected artifacts — failing to retry"
    exit 1
fi

echo "Downloading..."
cat /tmp/artifact-list.txt | parallel --will-cite -j 0 \
    "aws s3 cp --no-progress 's3://${S3_BUCKET}/{}' build-artifacts/"

# Count downloaded files
actual=$(find build-artifacts -type f | grep -E 'deb$|rpm$' | wc -l)
echo "Downloaded $actual / $expected artifacts"

if [ "$actual" -ne "$expected" ]; then
    echo "Not all artifacts downloaded — failing this attempt"
    exit 1
fi

echo "All artifacts downloaded successfully 🎉"
