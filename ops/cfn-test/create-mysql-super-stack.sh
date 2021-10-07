#!/usr/bin/env bash
set -eux -o pipefail

CFN_BUCKET=${CFN_BUCKET:-readysettech-tcat-harley-us-east-2}
AWS_REGION=${AWS_REGION:-us-east-2}
key_pair_name=$1
external_ip_address=$(curl ifconfig.me)
stack_name=${2:-${key_pair_name}-super}

parameters=(
  "ParameterKey=KeyPairName,ParameterValue=${key_pair_name}"
  "ParameterKey=AvailabilityZones,ParameterValue='us-east-2a,us-east-2b,us-east-2c'"
  "ParameterKey=AccessCIDR,ParameterValue=${external_ip_address}/32"
  "ParameterKey=ReadysetS3BucketName,ParameterValue=${CFN_BUCKET}"
  "ParameterKey=DatabaseUsername,ParameterValue=readyset"
  "ParameterKey=DatabasePassword,ParameterValue=readyset"
  "ParameterKey=DatabaseName,ParameterValue=readyset"
  "ParameterKey=DeploymentName,ParameterValue=${key_pair_name}"
)

aws cloudformation create-stack \
  --stack-name "$stack_name" \
  --template-url "https://${CFN_BUCKET}.s3.${AWS_REGION}.amazonaws.com/readyset/templates/readyset-mysql-super-template.yaml" \
  --parameters "${parameters[@]}" \
  --capabilities CAPABILITY_IAM
