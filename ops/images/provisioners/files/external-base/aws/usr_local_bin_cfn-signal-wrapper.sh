#!/bin/sh
# This script handles running cfn-signal if the correct environment variables are
# set and otherwise just returns success.
# Takes a single parameter the exit code to pass on.

set -eux

exit_code=$1

if [ -z "${AWS_CLOUDFORMATION_STACK+x}" ] || [ -z "${AWS_CLOUDFORMATION_RESOURCE+x}" ]; then
  exit 0
fi

/opt/aws-cfn/bin/python3 /opt/aws-cfn/bin/cfn-signal \
  --exit-code "$exit_code" \
  --stack "$AWS_CLOUDFORMATION_STACK" \
  --resource "$AWS_CLOUDFORMATION_RESOURCE" \
  --region "$AWS_CLOUDFORMATION_REGION"

