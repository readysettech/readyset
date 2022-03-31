#!/bin/bash
set -euf -x -o pipefail
shopt -s nullglob # have globs expand to nothing when they don't match

# -------------- [ Functions ] ---------------------------------- #

# Capture errors and always try to cleanup CloudFormation stack
#
on_error() {
  cleanup_resources
}

# https://buildkite.com/docs/agent/v3/configuration#cancel-signal
# Affords us the opportunity to cleanup before the calling code exits
trap 'on_error' SIGTERM
trap 'on_error' ERR

# Centralized function to execute cleanup routines
cleanup_resources() {
  echo "Attempting cleanup of CloudFormation stack."
  delete_cfn_stack "${STACK_NAME}"
}

delete_cfn_stack() {
  stack_name=$1
  set +e
  aws cloudformation delete-stack \
    --region "${AWS_REGION}" \
    --stack-name "${stack_name}"
  set -e
}

await_stack_completion() {
  echo "+++ Waiting for CloudFormation stack: ${STACK_ID} to finish provisioning."
  set +e
  # Polls every 30 seconds until a successful state has been reached.
  # This will exit with a return code of 255 after 120 failed checks.
  # https://go.aws/3pMIsdw
  o=$(aws cloudformation \
    wait \
    stack-create-complete \
    --stack-name "${STACK_ID}" \
    --region "${AWS_REGION}")
  ec=$?
  set -e
  echo "+++ :cloud: Checking benchmark superstack in CFN"
  if [[ "${ec}" == "255" && "${o}" == *"CREATE_FAILED"* ]]; then
    echo "Stack failed to provision."
    cleanup_resources
    exit $ec
  elif [[ "${ec}" == "255" ]]; then
    echo "Stack did not finish provisioning after 60-minutes so we're exiting with error."
    cleanup_resources
    exit $ec
  elif [[ "${ec}" != "0" ]]; then
    echo "Failed to establish whether stack came up healthy. Exiting with error."
    exit $ec
  fi
  echo "Stack appears to be up and ready!"
}

deploy_benchmark_cfn_stack() {
  echo "+++ :cloud: Launching benchmark superstack in CFN"
  set +e
  STACK=$(aws cloudformation create-stack \
    --region "${AWS_REGION}" \
    --stack-name "${STACK_NAME}" \
    --template-url "https://${CFN_BUCKET}.s3.${AWS_REGION}.amazonaws.com/${BUILDKITE_COMMIT}/readyset/templates/benchmark-super-template.yaml" \
    --parameters "${CFN_STACK_PARAMS[@]}" \
    --capabilities CAPABILITY_IAM \
    --disable-rollback
  )
  ec=$?
  set -e
  STACK_ID=$(echo $STACK | jq -r .StackId)
  # Valid Stack ARN will contain this string
  if ! [[ "${STACK_ID}" == *"stack/"* ]]; then
    echo "Failed to launch ReadySet benchmarking application stack."
    exit $ec
  fi
}

# ------------- [ Init ]----------------------------------------- #

CFN_BUCKET="${CFN_BUCKET:-readysettech-cfn-internal-us-east-2}"
BUILD_ID="${BUILDKITE_BUILD_NUMBER:-2}"
SHORT_COMMIT_SHA="${BUILDKITE_COMMIT:0:7}"
AWS_REGION="${AWS_REGION:-us-east-2}"
STACK_NAME="${STACK_NAME:-ci-benchmarks-$BUILD_ID}"
# Discovered during aws cloudformation create-stack response
STACK_ID=""

CFN_STACK_PARAMS=(
  "ParameterKey=ReadySetDeploymentName,ParameterValue=ReadySet-${BUILD_ID}-${SHORT_COMMIT_SHA}"
  "ParameterKey=ReadySetS3BucketName,ParameterValue=${CFN_BUCKET}"
  "ParameterKey=ReadySetS3KeyPrefix,ParameterValue=/${BUILDKITE_COMMIT}/readyset/"
  "ParameterKey=VPCCIDR,ParameterValue=10.3.128.0/18"
  'ParameterKey=VPCPrivateSubnetIds,ParameterValue="subnet-059f6d5839e5594db,subnet-0eeafdbcbec87c541,subnet-0def839fa1d92853c"'
  "ParameterKey=AdditionalAdapterCIDR,ParameterValue=10.3.128.0/18"
  "ParameterKey=DatabaseAllocatedStorage,ParameterValue=256"
  "ParameterKey=EnableMySQLReadySet,ParameterValue=true"
  "ParameterKey=MySQLRDSSnapshotId,ParameterValue=arn:aws:rds:us-east-2:305232526136:snapshot:readyset-mysql-8027-benchmarkdata-03-03-22-01"
  "ParameterKey=MySQLDatabaseName,ParameterValue=test"
  "ParameterKey=MySQLDatabaseUsername,ParameterValue=readyset"
  "ParameterKey=MySQLSSMPathRDSDatabasePassword,ParameterValue=/rs-build-us-east-2/benchmark/mysql/rdsPassword"
  "ParameterKey=EnablePostgreSQLReadySet,ParameterValue=false"
  "ParameterKey=PostgreSQLDatabaseName,ParameterValue=test"
  "ParameterKey=PostgreSQLDatabaseUsername,ParameterValue=readyset"
  "ParameterKey=PostgreSQLSSMPathRDSDatabasePassword,ParameterValue=/rs-build-us-east-2/benchmark/postgres/rdsPassword"
  "ParameterKey=SSHSecurityGroupID,ParameterValue=sg-0c020c99aa1fb4c2d"
  "ParameterKey=VPCID,ParameterValue=vpc-0adb26542fc16ab14"
  "ParameterKey=KeyPairName,ParameterValue=readyset-devops"
  "ParameterKey=ReadySetAdapterNodes,ParameterValue=3"
  "ParameterKey=ReadySetAdapterInstanceType,ParameterValue=m5.large"
  "ParameterKey=ReadySetMonitorInstanceType,ParameterValue=m5.large"
  "ParameterKey=ReadySetServerNodes,ParameterValue=3"
  "ParameterKey=ReadySetServerInstanceType,ParameterValue=c5.large"
  "ParameterKey=SSMParameterKmsKeyArn,ParameterValue=arn:aws:kms:us-east-2:069491470376:key/6de170fc-5105-4b97-a069-ca72296fdd59"
  'ParameterKey=ReadySetServerExtraEnvironment,ParameterValue="EXPERIMENTAL_PAGINATE_SUPPORT=true,EXPERIMENTAL_TOPK_SUPPORT=true"'
)

# ------------- [ Main ]--------------------------------- #

# Kick off stack creation for benchmark app environment
deploy_benchmark_cfn_stack

# Wait for env to finish provisioning
await_stack_completion
