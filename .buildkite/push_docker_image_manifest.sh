#!/usr/bin/env bash

# Create and push manifests for multi-architecture Docker images to an AWS ECR registry. Given an
# image name (i.e. "readyset-rust-ubuntu2204-builder-base"), create a multi-arch manifest tagged
# with $VERSION that references the architecture-specific images for that image name (i.e.
# "readyset-rust-ubuntu2204-builder-base:$VERSION-amd64").  If $BUILDKITE_BRANCH is defined and
# equals "refs/heads/main", also push the manifest with tag "latest".
#
# Environment variables used:
#   AWS_ACCOUNT      - AWS account ID (default: 305232526136, i.e. "readyset-build-default")
#   AWS_REGION       - AWS region (default: us-east-2)
#   VERSION          - Image version tag (default: $BUILDKITE_COMMIT)
#   BUILDKITE_BRANCH - CI branch name (used to determine if 'latest' tag is pushed)

set -o errexit
set -o nounset
set -o pipefail

ME=$(basename "$0")

if [[ $# -lt 1 ]]; then
  echo "Usage: $ME <image_name>"
  exit 1
fi

image_name=$1
archs=("amd64" "arm64")

AWS_ACCOUNT=${AWS_ACCOUNT:-305232526136}
AWS_REGION=${AWS_REGION:-us-east-2}
VERSION=${VERSION:-$BUILDKITE_COMMIT}

docker_repo="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
tag_prefix="$docker_repo/$image_name"

# Create a new array in which each architecture in the archs array is prefixed with the image tag
# (i.e. "arm64" becomes "docker_repo/image_name:version-arm64").
arch_images=("${archs[@]/#/$tag_prefix:$VERSION-}")

create_and_push_manifest() {
  local tag=$1
  local manifest="$tag_prefix:$tag"
  echo "Creating manifest: $manifest"
  docker manifest create "$manifest" "${arch_images[@]}"
  echo "Pushing manifest: $manifest"
  docker manifest push "$manifest"
}

echo "+++ :docker: Creating multi-arch manifest"
echo "Images to link:"
printf '  %s\n' "${arch_images[@]}"

create_and_push_manifest "$VERSION"

if [[ "${BUILDKITE_BRANCH:-}" = "refs/heads/main" ]]; then
  create_and_push_manifest "latest"
fi
