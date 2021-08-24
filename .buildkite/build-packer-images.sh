#!/bin/bash

set -euo pipefail

IMAGES_DIR="ops/images"
MAX_FAILED_JOB_RETRIES="3"
SHORT_COMMIT_ID=$(echo ${BUILDKITE_COMMIT} | cut -c 1-7)
RELEASE_VERSION=$(.buildkite/latest-release-version.sh)

echo "steps:"

if [ -n "${ONLY_VALIDATE+x}" ]; then
  # A step to validate each Packer image
  find ${IMAGES_DIR}/* -type f -name '*.pkr.hcl' | while read -r image; do
    PACKER_IMAGE=$(basename $(dirname $image))
    echo "  - label: ':packer: Validate Packer Image ($PACKER_IMAGE)'"
    echo "    key: validate-packer-image-${PACKER_IMAGE}"
    echo "    depends_on:"
    echo "      - build-ops-image"
    echo "    branches: \"!master !refs/heads/main\""
    echo "    command:"
    echo "      - cd ${IMAGES_DIR}/${PACKER_IMAGE} && mkdir binaries && ls -lah && pwd"
    echo "      - export BUILDKITE_BUILD_ID=\"$RELEASE_VERSION\""
    echo "      - buildkite-agent artifact download target/release/* binaries/"
    echo "      - packer fmt -check -diff ."
    echo "      - packer validate ."
    echo "    plugins:"
    echo "      - docker#v3.7.0:"
    echo "          image: 305232526136.dkr.ecr.us-east-2.amazonaws.com/readyset-ops:latest"
    echo "      - ecr#v2.2.0:"
    echo "          login: true"
    echo
  done
  exit 0
fi

# A step to do a dry run build of each Packer image
find ${IMAGES_DIR}/* -type f -name '*.pkr.hcl' | while read -r image; do
  PACKER_IMAGE=$(basename $(dirname $image))
  echo "  - label: ':packer: Build Packer Image ($PACKER_IMAGE)'"
  echo "    key: build-packer-image-${PACKER_IMAGE}"
  echo "    depends_on:"
  echo "      - build-ops-image"
  echo "    retry:"
  echo "      automatic:"
  echo "        - exit_status: \"*\""
  echo "          limit: ${MAX_FAILED_JOB_RETRIES}"
  echo "    branches: \"!master !refs/heads/main\""
  echo "    command:"
  echo "      - cd ${IMAGES_DIR}/${PACKER_IMAGE} && mkdir binaries && ls -lah && pwd"
  echo "      - export BUILDKITE_BUILD_ID=\"$RELEASE_VERSION\""
  echo "      - buildkite-agent artifact download target/release/* binaries/"
  echo "      - packer build ."
  echo "    plugins:"
  echo "      - docker#v3.7.0:"
  echo "          image: 305232526136.dkr.ecr.us-east-2.amazonaws.com/readyset-ops:latest"
  echo "          environment:"
  echo "          - \"PKR_VAR_skip_create_ami=true\""
  echo "          - \"PKR_VAR_short_commit_id=${SHORT_COMMIT_ID}\""
  echo "      - ecr#v2.2.0:"
  echo "          login: true"
  echo
done

# A step for building each Packer image (Actual release)
# The actual build and release will be done in master or main branch
find ${IMAGES_DIR}/* -type f -name '*.pkr.hcl' | while read -r image; do
  PACKER_IMAGE=$(basename $(dirname $image))
  echo "  - label: ':packer: Release Packer image ($PACKER_IMAGE)'"
  echo "    key: release-packer-image-${PACKER_IMAGE}"
  echo "    depends_on:"
  echo "      - build-ops-image"
  echo "    retry:"
  echo "      automatic:"
  echo "        - exit_status: \"*\""
  echo "          limit: ${MAX_FAILED_JOB_RETRIES}"
  echo "    branches: \"master refs/heads/main\""
  echo "    command:"
  echo "      - cd ${IMAGES_DIR}/${PACKER_IMAGE} && mkdir binaries && ls -lah && pwd"
  echo "      - export BUILDKITE_BUILD_ID=\"$RELEASE_VERSION\""
  echo "      - buildkite-agent artifact download target/release/* binaries/"
  echo "      - packer build ."
  echo "    plugins:"
  echo "      - docker#v3.7.0:"
  echo "          image: 305232526136.dkr.ecr.us-east-2.amazonaws.com/readyset-ops:latest"
  echo "          environment:"
  echo "          - \"PKR_VAR_short_commit_id=${SHORT_COMMIT_ID}\""
  echo "      - ecr#v2.2.0:"
  echo "          login: true"
  echo
done
