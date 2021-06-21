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
    echo "  - label: ':packer: Validate Packer image ($PACKER_IMAGE)'"
    echo "    key: validate-packer-image-${PACKER_IMAGE}"
    echo "    depends_on:"
    echo "      - packer-lint"
    echo "    branches: \"!master !main\""
    echo "    command:"
    echo "      - mkdir binaries && ls -lah && pwd"
    echo "      - export BUILDKITE_BUILD_ID=\"$RELEASE_VERSION\""
    echo "      - buildkite-agent artifact download target/release/* binaries/"
    echo "      - packer validate ."
    echo "    plugins:"
    echo "      - docker#v3.7.0:"
    echo "          image: 'hashicorp/packer:latest'"
    echo "          mount-checkout: false"
    echo "          entrypoint: \"\""
    echo "          shell: [\"/bin/bash\", \"-e\", \"-c\"]"
    echo "          workdir: /workspace"
    echo "          volumes:"
    echo "            - './${IMAGES_DIR}/${PACKER_IMAGE}:/workspace'"
    echo
  done
  exit 0
fi

# A step to do a dry run build of each Packer image
find ${IMAGES_DIR}/* -type f -name '*.pkr.hcl' | while read -r image; do
  PACKER_IMAGE=$(basename $(dirname $image))
  echo "  - label: ':packer: Build Packer image ($PACKER_IMAGE)'"
  echo "    key: build-packer-image-${PACKER_IMAGE}"
  echo "    retry:"
  echo "      automatic:"
  echo "        - exit_status: \"*\""
  echo "          limit: ${MAX_FAILED_JOB_RETRIES}"
  echo "    branches: \"!master !main\""
  echo "    command:"
  echo "      - mkdir binaries && ls -lah && pwd"
  echo "      - export BUILDKITE_BUILD_ID=\"$RELEASE_VERSION\""
  echo "      - buildkite-agent artifact download target/release/* binaries/"
  echo "      - packer build ."
  echo "    plugins:"
  echo "      - docker#v3.7.0:"
  echo "          image: 'hashicorp/packer:latest'"
  echo "          mount-checkout: false"
  echo "          entrypoint: \"\""
  echo "          shell: [\"/bin/bash\", \"-e\", \"-c\"]"
  echo "          workdir: /workspace"
  echo "          environment:"
  echo "          - \"PKR_VAR_skip_create_ami=true\""
  echo "          - \"PKR_VAR_short_commit_id=${SHORT_COMMIT_ID}\""
  echo "          volumes:"
  echo "            - './${IMAGES_DIR}/${PACKER_IMAGE}:/workspace'"
  echo
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
  echo "    branches: \"master main\""
  echo "    command:"
  echo "      - mkdir binaries && ls -lah && pwd"
  echo "      - export BUILDKITE_BUILD_ID=\"$RELEASE_VERSION\""
  echo "      - buildkite-agent artifact download target/release/* binaries/"
  echo "      - packer build ."
  echo "    plugins:"
  echo "      - docker#v3.7.0:"
  echo "          image: 'hashicorp/packer:latest'"
  echo "          mount-checkout: false"
  echo "          entrypoint: \"\""
  echo "          shell: [\"/bin/bash\", \"-e\", \"-c\"]"
  echo "          workdir: /workspace"
  echo "          environment:"
  echo "          - \"PKR_VAR_short_commit_id=${SHORT_COMMIT_ID}\""
  echo "          volumes:"
  echo "            - './${IMAGES_DIR}/${PACKER_IMAGE}:/workspace'"
  echo
done
