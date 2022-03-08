#!/bin/bash -ex
# The purpose of this script is to gather all necessary dependencies for running
# Noria benchmarks in docker-compose. This process downloads a prebuilt MySQL
# datadir which was populate using the data generator that's included in this
# repository.

ECR_AWS_ACCOUNT_ID=305232526136
ECR_AWS_REGION="us-east-2"
ECR_REPO_MYSQL="${ECR_AWS_ACCOUNT_ID}.dkr.ecr.${ECR_AWS_REGION}.amazonaws.com"

# Environment file containing configurations for benchmarking service in BENCHMARK_DC_FILE_PATH
BENCHMARK_DC_ENV_PATH=${BENCHMARK_DC_ENV_PATH:-"./.buildkite/benchmark.env"}

if [ -e "${BENCHMARK_DC_ENV_PATH}" ]; then
  set +e
  . ./.buildkite/benchmark.env
  ec=$?
  set -e
  if [ $ec -gt 0 ]; then
    echo "Unable to load environment from benchmark env file at: ${BENCHMARK_DC_ENV_PATH}. Exiting with error."
    exit 1
  fi
else
  echo "No env file detected at ${BENCHMARK_DC_ENV_PATH}. Exiting with error."
  exit 1
fi

# Location of docker-compose file belonging to benchmarks
BENCHMARK_DC_FILE_PATH=${BENCHMARK_DC_FILE_PATH:-"./.buildkite/docker-compose.readyset-benchmark-mysql80.yml"}
# S3 bucket name and key to the DATADIR archive containing pre-built benchmark databases
BENCHMARK_DATADIR_S3_KEY=${BENCHMARK_DATADIR_S3_KEY:-"readyset-devops-assets-build-us-east-2/benchmark-datadir-1.4.tgz"}
# Location to save downloaded compressed mysql datadir archive
BENCHMARK_DATADIR_ARCHIVE_SAVE_AS=${BENCHMARK_DATADIR_ARCHIVE_SAVE_AS:-"/tmp/mysql-datadir.tgz"}
# Where the automation should dump the downloaded MySQL datadir archive for mounting into docker-compose later
BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO=${BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO:-"/tmp/mysqldatadir"}
# Where benchmark report files should be saved to
BENCHMARK_ARTIFACT_DIR=${BENCHMARK_ARTIFACT_DIR:-"/tmp/artifacts"}
# If set to 'enabled' and BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO already exists, datadir will not be replaced
BENCHMARK_DATADIR_CACHING=${BENCHMARK_DATADIR_CACHING:-"disabled"}
# Name of the docker-compose project to run benchmarks within the context of
BENCHMARK_DC_PROJECT_NAME="${BENCHMARK_DC_PROJECT_NAME:-benchmarks}"
# Maximum amount of time to wait for containers to stop before killing
BENCHMARK_DC_RUN_TIMEOUT="${BENCHMARK_DC_RUN_TIMEOUT:-10}"
LOAD_AWS_PROFILE="${AWS_PROFILE:-build-admin}"

show_benchmark_container_logs() {
  set +e
  docker-compose -p ${BENCHMARK_DC_PROJECT_NAME} logs benchmark
  set -e
}

show_db_container_logs() {
  set +e
  docker-compose -p ${BENCHMARK_DC_PROJECT_NAME} logs mysql
  docker-compose -p ${BENCHMARK_DC_PROJECT_NAME} logs readyset-server
  docker-compose -p ${BENCHMARK_DC_PROJECT_NAME} logs readyset-adapter
  set -e
}

local_ecr_login () {
  # Authenticate with ECR if this is local
  if [ "${ENVIRONMENT}" == "localdevelopment" ]; then
    aws ecr get-login-password --region ${ECR_AWS_REGION} --profile ${LOAD_AWS_PROFILE} | docker login -uAWS --password-stdin ${ECR_REPO_MYSQL}
  fi
}

echo "Stopping docker-compose stack, just in case."
docker-compose \
  -f ${BENCHMARK_DC_FILE_PATH} \
  -p ${BENCHMARK_DC_PROJECT_NAME} \
  --env-file ${BENCHMARK_DC_ENV_PATH} \
  down -v

echo '+++ MySQL Datadir Setup'
echo 'Extracting downloaded MySQL datadir for use in Docker Compose.'

if [[ "${BENCHMARK_DATADIR_CACHING}" == "enabled" && -e $BENCHMARK_DATADIR_ARCHIVE_SAVE_AS ]]; then
  echo "Skipping MySQL datadir download since we already have one locally and caching is enabled via BENCHMARK_DATADIR_CACHING."
else
  echo "Creating directory to store downloaded MySQL data directory."
  # Download latest benchmark datadir
  echo "Downloading latest DB benchmark data from S3."
  if [ "${ENVIRONMENT}" == "localdevelopment" ]; then
    aws s3 cp s3://${BENCHMARK_DATADIR_S3_KEY} ${BENCHMARK_DATADIR_ARCHIVE_SAVE_AS} --profile ${LOAD_AWS_PROFILE} --quiet
  else
    # Try to download quietly, but if unable, show errors.
    aws s3 cp s3://${BENCHMARK_DATADIR_S3_KEY} ${BENCHMARK_DATADIR_ARCHIVE_SAVE_AS} --quiet || aws s3 cp s3://${BENCHMARK_DATADIR_S3_KEY} ${BENCHMARK_DATADIR_ARCHIVE_SAVE_AS}
  fi

  mkdir -p ${BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO}
  tar -xf ${BENCHMARK_DATADIR_ARCHIVE_SAVE_AS} -C ${BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO}

  # Necessary to allow mysql container to r/w to this directory in CI
  if [ "${ENVIRONMENT}" == "ci" ]; then
    sudo chown -R 999:999 ${BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO}
    sudo chmod -R 0777 ${BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO}
    sudo rm -f ${BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO}/auto.cnf
  fi
fi

echo "HOST_DATADIR_PATH=${BENCHMARK_DATADIR_ARCHIVE_EXTRACT_TO}" >> ${BENCHMARK_DC_ENV_PATH}
echo "HOST_ARTIFACT_PATH=${BENCHMARK_ARTIFACT_DIR}" >> ${BENCHMARK_DC_ENV_PATH}

set +e
echo '+++ Execute Benchmarks'
echo 'Running benchmarks in docker-compose'
export PYTHONUNBUFFERED=1
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1
docker-compose \
  -f ${BENCHMARK_DC_FILE_PATH} \
  --env-file ${BENCHMARK_DC_ENV_PATH} \
  -p ${BENCHMARK_DC_PROJECT_NAME} \
  up \
  --exit-code-from benchmarks \
  --timeout ${BENCHMARK_DC_RUN_TIMEOUT} \
  --remove-orphans

benchmark_ec=$?

echo '+++ Benchmark Results'
if [ $benchmark_ec -gt 0 ]; then
    echo 'Benchmarking stack exited with a failure exit code. Whoops, we should look into that. Exiting with failure.'
    echo '+++ Display Container Logs'
    show_db_container_logs
    show_benchmark_container_logs
    set -e
    exit $benchmark_ec
fi
echo ":tada: Finished successfully."
