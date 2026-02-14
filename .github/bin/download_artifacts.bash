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

# Determine expected artifact count - manually triggerd
if [ "$GH_EVENT" = "workflow_dispatch" ]; then
    files=$(aws s3 ls "$S3_PATH" --recursive | awk '{print $4}' | grep -E 'deb$|rpm$' || true)
    if [ -z "$files" ]; then
        echo "ERROR: No artifacts found for version ${VERSION}" >&2
        exit 1
    fi
    echo "$files" >/tmp/artifact-list.txt
    expected=$(wc -l </tmp/artifact-list.txt)
else
    # Triggered automatically via commit or PR to special branches.
    expected=${COUNT:-0}
fi
# Error out if nothing to expect
if [ "$expected" -le 0 ]; then
    echo "ERROR: Expected artifact count is $expected" >&2
    exit 1
fi

echo "Expecting $expected artifacts"

# Download all artifacts
files=$(aws s3 ls "$S3_PATH" --recursive | awk '{print $4}' | grep -E 'deb$|rpm$' || true)
echo "$files" >/tmp/artifact-list.txt
completed=$(wc -l </tmp/artifact-list.txt)

if [ "$completed" -lt "$expected" ]; then
    echo "Have $completed / $expected artifacts — failing to retry"
    exit 1
fi

echo "Downloading..."
cat /tmp/artifact-list.txt | parallel --will-cite -j 0 \
    "aws s3 cp --no-progress 's3://${S3_BUCKET}/{}' build-artifacts/"

# Count downloaded files
actual=$(find build-artifacts -type f | grep -E 'deb$|rpm$' | wc -l)
echo "Have $actual / $expected artifacts"

if [ "$actual" -ne "$expected" ]; then
    echo "Not all artifacts downloaded — failing this attempt"
    exit 1
fi

echo "All artifacts downloaded successfully 🎉"
