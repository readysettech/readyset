#!/bin/sh
# This script handles running cfn-signal if the correct environment variables are
# set and otherwise just returns success.
# Takes a single parameter the exit code to pass on.

set -eux

exit_code=$1

if [ -n "${AWS_CLOUDFORMATION_STACK:+x}" ] || [ -n "${AWS_CLOUDFORMATION_CONFIGSETS:+x}" ]; then
  exit 0
fi

/opt/aws-cfn/bin/cfn-init \
  --exit-code "$exit_code" \
  --stack "$AWS_CLOUDFORMATION_STACK" \
  --configsets "$AWS_CLOUDFORMATION_CONFIGSETS" \
  --region "$AWS_CLOUDFORMATION_REGION"

