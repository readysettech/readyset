#!/usr/bin/env bash
set -eux -o pipefail

# Starts the creation of a ReadySet MySQL super stack using CloudFormation.

HELP_TEXT="
Arguments:
  -h | hash | Git Commit Hash
  -k | key | SSH Key pair name

Example usage:
  create-mysql-super-stack.sh \
    -h ed7db94a0b1f440bfdee98f0d0247c3ae27bf092 \
    -k testkey
"
VERSION_NUM="0.0.1"

while getopts h:k:v flag
do
    case "${flag}" in
        h) hash=${OPTARG};;
        k) key=${OPTARG};;
        v) echo "create-mysql-super-stack version: $VERSION_NUM"; exit;;
        *) echo "$HELP_TEXT"; exit;;
    esac
done

CFN_BUCKET=${CFN_BUCKET:-readysettech-cfn-internal-us-east-2}
CFN_BUCKET=${CFN_BUCKET:-readysettech-cfn-internal-us-east-2}
AWS_REGION=${AWS_REGION:-us-east-2}

commit=$hash
key_pair_name=$key
external_ip_address=$(curl ifconfig.me)
stack_name="${key_pair_name}-${commit}-super"

parameters=(
  "ParameterKey=KeyPairName,ParameterValue=${key_pair_name}"
  "ParameterKey=AvailabilityZones,ParameterValue='us-east-2a,us-east-2b,us-east-2c'"
  "ParameterKey=AccessCIDR,ParameterValue=${external_ip_address}/32"
  "ParameterKey=ReadySetS3BucketName,ParameterValue=${CFN_BUCKET}"
  "ParameterKey=ReadySetS3KeyPrefix,ParameterValue=/${commit}/readyset/"
  "ParameterKey=DatabaseUsername,ParameterValue=readyset"
  "ParameterKey=DatabaseName,ParameterValue=readyset"
  "ParameterKey=ReadySetDeploymentName,ParameterValue=${stack_name}"
  "ParameterKey=AdditionalAdapterCIDR,ParameterValue=10.0.0.0/18"
  "ParameterKey=SSMParameterKmsKeyArn,ParameterValue=arn:aws:kms:us-east-2:069491470376:key/5cb3afeb-e9dd-40df-98cf-a82bb53ee78b"
  "ParameterKey=SSMPathRDSDatabasePassword,ParameterValue=/readyset/sandbox/dbPassword"
)

aws cloudformation create-stack \
  --region "$AWS_REGION" \
  --stack-name "$stack_name" \
  --template-url "https://${CFN_BUCKET}.s3.${AWS_REGION}.amazonaws.com/${commit}/readyset/templates/readyset-mysql-super-template.yaml" \
  --parameters "${parameters[@]}" \
  --disable-rollback \
  --capabilities CAPABILITY_IAM

echo "$stack_name"
