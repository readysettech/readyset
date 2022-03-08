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
trap 'on_error' SIGTERM
trap 'on_error' ERR

# Helper function. Errors if the provided input $1 is empty.
error_if_empty() {
  input=$1
  name=$2
  if [ -z "$input" ]; then
    echo "Failed to get a value for $name"
    exit 1
  fi
}

# Deletes the named CloudFormation stack $1 and ignores errors
delete_cfn_stack() {
  stack_name=$1
  set +e
  aws cloudformation delete-stack \
    --region "${AWS_REGION}" \
    --stack-name "${stack_name}"
  set -e
}

# Centralized function to execute cleanup routines
cleanup_resources() {
  echo "Attempting cleanup of CloudFormation stack."
  delete_cfn_stack "${STACK_NAME}"
  cleanup_runner_job "${job_name}"
  # Cleanup kubeconfig
  rm "${BUILDKITE_BUILD_CHECKOUT_PATH}/${EKS_CLUSTER_NAME}"
}

# Downloads the Helm binary for usage on the current machine, if not present already
setup_helm() {
  if [[ $(type -P "helm") ]]; then
    echo "Helm already installed in PATH. Moving on."
    return
  fi
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
}

# Downloads portable kubectl binary if necessary.
setup_kubectl() {
  if [[ $(type -P "kubectl") ]]; then
    echo "kubectl already installed in PATH. Moving on."
    return
  fi
  curl -o kubectl "${KUBECTL_BIN_URL}"
  chmod +x kubectl
  mkdir -p ~/.local/bin
  mv ./kubectl ~/.local/bin/kubectl
}

# Generates a kubeconfig file for authenticating with the EKS cluster
setup_kubeconfig() {
  echo "Generating kubeconfig file for cluster: ${EKS_CLUSTER_NAME}"
  aws eks update-kubeconfig \
    --region ${AWS_REGION} \
    --name ${EKS_CLUSTER_NAME} \
    --kubeconfig "${BUILDKITE_BUILD_CHECKOUT_PATH}/${EKS_CLUSTER_NAME}"
}

# Waits for the benchmark runner job to either pass or fail within the alotted time.
await_job_completion() {
  local job_name=$1
  local i=1
  local max_loops=360
  local sleep_between_sec=10
  local job_result=128

  while [[ $i -le $max_loops ]]; do
    echo "Checking state of job: $i / $max_loops"
    if kubectl wait --for=condition=complete --timeout=0 job/$job_name 2>/dev/null; then
      job_result=0
      break
    fi

    if kubectl wait --for=condition=failed --timeout=0 job/$job_name 2>/dev/null; then
      job_result=1
      break
    fi
    i=$((i+1))
    sleep $sleep_between_sec
  done

  if [[ $i -eq $max_loops && $job_result -ne 0 ]]; then
    echo "Timed out waiting for the benchmark runner job to complete. Something may have went wrong."
    cleanup_resources
    exit 1
  elif [[ $job_result -ne 0 ]]; then
    echo "Benchmark runner job failed to execute successfully."
    get_job_logs "${RUNNER_JOB_NAME}"
    cleanup_resources
    exit 1
  fi

  echo "Job appears to have completed successfully. Moving on."
}

# Ensures the runner job was created in k8s. Exits if the job wasn't found post-creation.
ensure_job_created_ok() {
  local job_name=$1
  # Quick initial check to ensure job was created properly
  job_status=$(kubectl get job/$job_name 2>&1)
  if [[ "${job_status}" == *"NotFound"* ]]; then
    echo "Job ${job_name} does not exist in k8s. This shouldn't happen. Exiting with error."
    exit 1
  fi
}

# Fetches logs from the most recent pod created by the k8s job.
get_job_logs() {
  local job_name=$1
  echo "+++ Fetching job logs post benchmarking"
  set +e
  pod_id=$(kubectl get pod -l job-name=$job_name --sort-by=.metadata.creationTimestamp -o 'jsonpath={.items[-1].metadata.name}')
  if [[ "${pod_id}" == *"${job_name}"* ]]; then
    kubectl logs $pod_id > "${RUNNER_JOB_LOG_FILE}"
    cat "${RUNNER_JOB_LOG_FILE}"
  else
    echo "Failed to locate job pod logs."
  fi
  set -e
}

# Deletes the specified kubernetes job.
cleanup_runner_job() {
  local job_name=$1
  echo "+++ :broom: Cleaning up k8s runner job"
  set +e
  kubectl delete job $job_name
  ec=$?
  set -e
  if [[ $ec -gt 0 ]]; then
    echo "Failed to cleanup old runner job: ${job_name}. It will need to be cleaned up manually."
    return
  fi
  echo "Job ${job_name} was deleted from ${RUNNER_K8S_NAMESPACE} ns successfully!"
}

# Parses result files from benchmark runner pod output, writes them to disk, and uploads as Buildkite artifact.
process_test_results() {
  # Extract combined benchmark reports from pod output
  results_json=$(echo $(cat $RUNNER_JOB_LOG_FILE) | sed -nr 's/.*BENCH_RUNNER_OUTPUT(.*)END.*/\1/p')
  if [[ -z "${results_json}" ]]; then
    echo "Unable to locate serialized benchmark report markers in pod runner output. This causes no artifacts to be uploaded."
    return
  fi
  echo $results_json | jq -r '.  | to_entries[] | [.key, .value] | @tsv' |
    while IFS=$'\t' read -r key val; do
      echo "Received runner output file: $key"
      # Safety check
      if [[ "$key" == *".log"* ]]; then
        # Decode the file's contents and save on disk
        echo "$val" | base64 -d > "./cfn-${key}"
        echo "Uploading result file ${key} into CI system artifacts."
        buildkite-agent artifact upload "./cfn-${key}"
      fi
    done
}

# ------------- [ Init ]----------------------------------------- #

#
# CI/CD system parameters
# If no build ID is provided, use a hash to avoid collisions
_RANDOM_ID=$(echo $RANDOM | md5sum | head -c 7)
# Overarching build ID
BUILD_ID="${BUILDKITE_BUILD_NUMBER:-$_RANDOM_ID}"

#
# CloudFormation Configs
CFN_BUCKET="${CFN_BUCKET:-readysettech-cfn-internal-us-east-2}"
AWS_REGION="${AWS_REGION:-us-east-2}"
STACK_NAME="${STACK_NAME:-ci-benchmarks-$BUILD_ID}"
PROM_INSTANCE_LABEL="ci-benchmark-${BUILDKITE_BUILD_NUMBER}-${BUILDKITE_COMMIT:0:7}"

#
# Kubernetes related configs
EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME:-rs-build-us-east-2}"
# Kubernetes namespace for runners to be launched in, within EKS_CLUSTER_NAME
RUNNER_K8S_NAMESPACE="${RUNNER_K8S_NAMESPACE:-build}"
# Readyset-benchmark container tag to deploy as runner
RUNNER_IMAGE_TAG="${RUNNER_IMAGE_TAG:-latest}"
# Save location for the rendered/templatized Helm template
RUNNER_K8S_YAML_PATH=./benchmark-job-manifest.yml

# S3 bucket where kubectl can be downloaded
KUBECTL_BIN_URL="https://amazon-eks.s3.us-west-2.amazonaws.com/1.21.2/2021-07-05/bin/linux/amd64/kubectl"
# Used for naming runner
RUNNER_RDBMS_NAME="${RUNNER_RDBMS_NAME:-mysql}"
# Name of the k8s job to be created for the runner
RUNNER_JOB_NAME="benchmark-${BUILD_ID}-${RUNNER_RDBMS_NAME}"
# Where for the job runner will be saved
RUNNER_JOB_LOG_FILE="${RUNNER_JOB_LOG_FILE:-${BUILDKITE_BUILD_CHECKOUT_PATH}/k8s-job-output.log}"

# ------------- [ Main ]--------------------------------- #

export PATH=$PATH:~/.local/bin/
export KUBECONFIG="${BUILDKITE_BUILD_CHECKOUT_PATH}/${EKS_CLUSTER_NAME}"

# Gather required outputs from the pre-deployed CFN stack
described_stack=$(aws cloudformation --region "${AWS_REGION}" describe-stacks --stack-name "${STACK_NAME}")
ec=$?
if [[ "${ec}" != "0" ]]; then
 echo "Failed to describe stack. It should have been created by a previous step."
 exit $ec
fi

# Deployed stack's MySQL username
mysql_adapter_db_user=$(echo "$described_stack" | jq -r '.Stacks[0].Parameters | .[] | select(.ParameterKey == "MySQLDatabaseUsername").ParameterValue')
error_if_empty "${mysql_adapter_db_user}" "mysql_adapter_db_user"

# Deployed stack's Adapter NLB
mysql_adapter_nlb_host=$(echo "$described_stack" | jq -r '.Stacks[0].Outputs | .[] | select(.OutputKey == "ReadySetMySQLAdapterNLBDNSName").OutputValue')
error_if_empty "${mysql_adapter_nlb_host}" "mysql_adapter_nlb_host"

# Deployed stack's RDS hostname
mysql_rds_host=$(echo "$described_stack" | jq -r '.Stacks[0].Outputs | .[] | select(.OutputKey == "MySQLRDSHostname").OutputValue')
error_if_empty "${mysql_rds_host}" "mysql_rds_host"

# Benchmark configurations
mysql_target_conn_str="mysql://$mysql_adapter_db_user:noriatesting@${mysql_adapter_nlb_host}:3306/test"
mysql_setup_conn_str="mysql://$mysql_adapter_db_user:noriatesting@${mysql_rds_host}:3306/test"
LABEL_APP="benchmark"

# Setup
setup_helm
setup_kubectl
setup_kubeconfig

# Interpolate Helm Templates
echo "+++ :helm: Generating Runner Job Template"
helm template ./benchmarks/provisioning/helm/charts/readyset-benchmarks/ \
  --set benchmark.runner.env.INSTANCE_LABEL="${PROM_INSTANCE_LABEL}" \
  --set benchmark.runner.env.TARGET_CONN_STR="${mysql_target_conn_str}" \
  --set benchmark.runner.env.SETUP_CONN_STR="${mysql_setup_conn_str}" \
  --set benchmark.runner.labels.app="${LABEL_APP}" \
  --set benchmark.runner.labels.buildNumber="${BUILD_ID}" \
  --set benchmark.runner.labels.rdbms="${RUNNER_RDBMS_NAME}" \
  --set benchmark.runner.image.tag="${RUNNER_IMAGE_TAG}" \
  --debug > "${RUNNER_K8S_YAML_PATH}"

ec=$?
if [[ "${ec}" != "0" ]]; then
  echo "Failed to generate Helm template for benchmark runner job. Cannot complete test."
  delete_cfn_stack "${STACK_NAME}"
  exit 1
fi

# Log template for future debugging purposes.
# No information here is truly sensitive.
echo "Succesfully generated template file for pod:"
cat "${RUNNER_K8S_YAML_PATH}"

echo "+++ :k8s: Launching benchmark runner pod in k8s"
kubectl config set-context --current --namespace=$RUNNER_K8S_NAMESPACE

echo "Cleaning up previous instances of the job, if it exists."
kubectl delete job ${RUNNER_JOB_NAME} || true

echo "Applying Kubernetes declarations"
kubectl apply -f ${RUNNER_K8S_YAML_PATH}

# Give K8S API server a moment
sleep 5
# Name of k8s job for benchmark runner
job_name="benchmark-${BUILD_ID}-${RUNNER_RDBMS_NAME}"

# Make sure that the k8s job exists as expected
ensure_job_created_ok "${RUNNER_JOB_NAME}"

echo "Waiting for benchmark runner pod to complete."
await_job_completion "${RUNNER_JOB_NAME}"

echo "+++ :cloud: Slurping benchmark runner logs"
get_job_logs "${RUNNER_JOB_NAME}"
set +e
process_test_results
set -e

echo "+++ :cloud: Cleaning up CFN and :k8s: resources"
cleanup_resources
