#!/bin/bash

set -euo pipefail

IMAGES_DIR="ops/images"
MAX_FAILED_JOB_RETRIES="3"
SHORT_COMMIT_ID=$(echo ${BUILDKITE_COMMIT} | cut -c 1-7)

echo "steps:"

echo "  - label: ':packer: Lint Packer configuration'"
echo "    key: packer-lint"
echo "    depends_on:"
echo "      - build-packer-images"
echo "    plugins:"
echo "      - docker#v3.7.0:"
echo "          image: 'hashicorp/packer:latest'"
echo "          command: [\"fmt\", \"-check\", \"-diff\", \"-recursive\", \"${IMAGES_DIR}\"]"
echo "          workdir: /workspace"

# Make sure all images wait until the Packer lint is successfull
echo "  - wait: ~"

# A step to validate each Packer image
find ${IMAGES_DIR}/* -type f -name '*.pkr.hcl' | while read -r image; do
  PACKER_IMAGE=$(basename $(dirname $image))
  echo "  - label: ':packer: Validate Packer image ($PACKER_IMAGE)'"
  echo "    key: validate-packer-image-${PACKER_IMAGE}"
  echo "    depends_on:"
  echo "      - packer-lint"
  echo "    branches: \"!master !main\""
  echo "    plugins:"
  echo "      - docker#v3.7.0:"
  echo "          image: 'hashicorp/packer:latest'"
  echo "          command: [\"validate\", \".\"]"
  echo "          mount-checkout: false"
  echo "          workdir: /workspace"
  echo "          volumes:"
  echo "            - '${BUILDKITE_BUILD_CHECKOUT_PATH}/${IMAGES_DIR}/${PACKER_IMAGE}:/workspace'"
done

# A step to do a dry run build of each Packer image
find ${IMAGES_DIR}/* -type f -name '*.pkr.hcl' | while read -r image; do
  PACKER_IMAGE=$(basename $(dirname $image))
  echo "  - label: ':packer: Build Packer image ($PACKER_IMAGE)'"
  echo "    key: build-packer-image-${PACKER_IMAGE}"
  echo "    retry:"
  echo "      automatic:"
  echo "        - exit_status: \"*\""
  echo "          limit: ${MAX_FAILED_JOB_RETRIES}"
  echo "    depends_on:"
  echo "      - validate-packer-image-${PACKER_IMAGE}"
  echo "    branches: \"!master !main\""
  echo "    plugins:"
  echo "      - docker#v3.7.0:"
  echo "          image: 'hashicorp/packer:latest'"
  echo "          command: [\"build\", \".\"]"
  echo "          mount-checkout: false"
  echo "          workdir: /workspace"
  echo "          environment:"
  echo "          - \"PKR_VAR_skip_create_ami=true\""
  echo "          - \"PKR_VAR_short_commit_id=${SHORT_COMMIT_ID}\""
  echo "          volumes:"
  echo "            - '${BUILDKITE_BUILD_CHECKOUT_PATH}/${IMAGES_DIR}/${PACKER_IMAGE}:/workspace'"
done

# A step for building each Packer image (Actual release)
# The actual build and release will be done in master or main branch
find ${IMAGES_DIR}/* -type f -name '*.pkr.hcl' | while read -r image; do
  PACKER_IMAGE=$(basename $(dirname $image))
  echo "  - label: ':packer: Release Packer image ($PACKER_IMAGE)'"
  echo "    key: release-packer-image-${PACKER_IMAGE}"
  echo "    retry:"
  echo "      automatic:"
  echo "        - exit_status: \"*\""
  echo "          limit: ${MAX_FAILED_JOB_RETRIES}"
  echo "    depends_on:"
  echo "      - packer-lint"
  echo "    branches: \"master main\""
  echo "    plugins:"
  echo "      - docker#v3.7.0:"
  echo "          image: 'hashicorp/packer:latest'"
  echo "          command: [\"build\", \".\"]"
  echo "          mount-checkout: false"
  echo "          workdir: /workspace"
  echo "          environment:"
  echo "          - \"PKR_VAR_short_commit_id=${SHORT_COMMIT_ID}\""
  echo "          volumes:"
  echo "            - '${BUILDKITE_BUILD_CHECKOUT_PATH}/${IMAGES_DIR}/${PACKER_IMAGE}:/workspace'"
done
