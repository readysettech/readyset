#!/bin/sh
set -eux

# Uploads the rendered CloudFormation template artifacts from a Buildkite step
# in TEMPLATE_ARTIFACT_STEP_NAME environment variable to the S3 Bucket and
# Prefix as defined by the environment variables S3_BUCKET and S3_PREFIX.

if [ -z ${TEMPLATE_ARTIFACT_STEP_NAME+x} ]; then
  echo "TEMPLATE_ARTIFACT_STEP_NAME not set"
  exit 1
fi

if [ -z ${S3_BUCKET+x} ]; then
  echo "S3_BUCKET not set"
  exit 1
fi

if [ -z ${S3_PREFIX+x} ]; then
  echo "S3_PREFIX not set"
  exit 1
fi

cd ops/cfn
mkdir ./rendered_templates
buildkite-agent artifact download "*.yaml" --step "${TEMPLATE_ARTIFACT_STEP_NAME}" ./rendered_templates
cd ./rendered_templates
aws s3 cp . "s3://${S3_BUCKET}/${S3_PREFIX}/readyset/templates/" \
  --exclude "*" \
  --include "*.yaml" \
  --acl bucket-owner-full-control \
  --recursive
