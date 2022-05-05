#!/usr/bin/env bash
set -euo pipefail

s3_bucket="s3://readysettech-orchestrator-us-east-2"
cloudfront_distribution_id="EGUPZSIU4Q3J9"

echo "+++ Downloading release binaries"
buildkite-agent artifact download target/x86_64-unknown-linux-musl/release/readyset-installer .
buildkite-agent artifact download target/x86_64-apple-darwin/release/readyset-installer .
buildkite-agent artifact download target/aarch64-apple-darwin/release/readyset-installer .

echo "+++ Uploading release binaries to S3"
aws s3 cp \
    target/x86_64-unknown-linux-musl/release/readyset-installer \
    "$s3_bucket/binaries/x86_64-unknown-linux-gnu" # this difference is intentional

aws s3 cp \
    target/x86_64-apple-darwin/release/readyset-installer \
    "$s3_bucket/binaries/x86_64-apple-darwin"

aws s3 cp \
    target/aarch64-apple-darwin/release/readyset-installer \
    "$s3_bucket/binaries/aarch64-apple-darwin"

echo "+++ Uploading wrapper script to S3"
aws s3 cp \
    installer/readyset-orchestrator.sh \
    "$s3_bucket/readyset-orchestrator.sh"

echo "+++ Creating CloudFront invalidation"
invalidation_id="$(
    aws cloudfront create-invalidation \
        --distribution-id "$cloudfront_distribution_id" \
        --paths '/*' \
        | jq -r '.Invalidation.Id'
)"

echo "Waiting for CloudFront invalidation to be complete"
while [ "$(aws cloudfront get-invalidation \
    --distribution-id "$cloudfront_distribution_id"  \
    --id "$invalidation_id" \
    | jq -r .Invalidation.Status)" = "InProgress" ]; do
          echo -n '.'
      done
echo
