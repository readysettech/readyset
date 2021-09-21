#!/bin/sh
# This script handles running cfn-init if the correct environment variables are
# set and otherwise just returns success.

set -eux

if [ -z "${AWS_CLOUDFORMATION_STACK:+x}" ]; then
  exit 0
fi

/opt/aws-cfn/bin/cfn-init \
  --stack "$AWS_CLOUDFORMATION_STACK" \
  --resource "$AWS_CLOUDFORMATION_RESOURCE" \
  --configsets "$AWS_CLOUDFORMATION_CONFIGSETS" \
  --region "$AWS_CLOUDFORMATION_REGION"

