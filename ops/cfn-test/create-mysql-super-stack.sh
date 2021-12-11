#!/usr/bin/env bash
set -eux -o pipefail

# Starts the creation of a ReadySet MySQL super stack using CloudFormation.

# This takes one parameter of the AWS EC2 SSH Key Pair name and uses it for
# access and as the name of the deployment.
# You can also override which bucket to fetch the templates from using the
# CFN_BUCKET environment variable.
# NOTE: This is currently pointed at Harley's taskcat bucket but should be
# moved elsewhere by default in the future.

CFN_BUCKET=${CFN_BUCKET:-readysettech-tcat-harley-us-east-2}
AWS_REGION=${AWS_REGION:-us-east-2}
key_pair_name=$1
external_ip_address=$(curl ifconfig.me)
stack_name=${2:-${key_pair_name}-super}

parameters=(
  "ParameterKey=KeyPairName,ParameterValue=${key_pair_name}"
  "ParameterKey=AvailabilityZones,ParameterValue='us-east-2a,us-east-2b,us-east-2c'"
  "ParameterKey=AccessCIDR,ParameterValue=${external_ip_address}/32"
  "ParameterKey=ReadySetS3BucketName,ParameterValue=${CFN_BUCKET}"
  "ParameterKey=DatabaseUsername,ParameterValue=readyset"
  "ParameterKey=DatabaseName,ParameterValue=readyset"
  "ParameterKey=ReadySetDeploymentName,ParameterValue=${key_pair_name}"
  "ParameterKey=SSMParameterKmsKeyArn,ParameterValue=arn:aws:kms:us-east-2:069491470376:key/5cb3afeb-e9dd-40df-98cf-a82bb53ee78b"
  "ParameterKey=SSMPathRDSDatabasePassword,ParameterValue=/readyset/sandbox/dbPassword"
)

aws cloudformation create-stack \
  --stack-name "$stack_name" \
  --template-url "https://${CFN_BUCKET}.s3.${AWS_REGION}.amazonaws.com/readyset/templates/readyset-mysql-super-template.yaml" \
  --parameters "${parameters[@]}" \
  --disable-rollback \
  --capabilities CAPABILITY_IAM
