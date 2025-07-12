#!/usr/bin/env bash
set -euo pipefail

# This script is used by Buildkite to standardize a few things about how we
# build Docker containers that are used for other steps in Buildkite.
# The script ensures the following things:
# Docker Buildkit is used for better performance and caching
# Builds are uploaded to our private AWS Elastic Container Registry
# When builds are done on main, they are tagged as latest to help with caching.

dockerfile="$1"
image_name="$2"
context="$3"

if [ -z "$dockerfile" ] || [ -z "$image_name" ]; then
    echo "Usage: $0 <dockerfile> <image_name> <context> [<docker-build-args>...]" >&2
    exit 1
fi
shift 3

AWS_ACCOUNT=${AWS_ACCOUNT:-305232526136} # build
AWS_REGION=${AWS_REGION:-us-east-2}
VERSION=${VERSION:-$BUILDKITE_COMMIT}

# Standardize detected arch variants to preferred docker style:
arch=$(arch | sed 's/x86_64/amd64/;s/aarch64/arm64/')

if [[ "$arch" != "arm64" && "$arch" != "amd64" ]]; then
    echo "Error: Unsupported architecture: $arch" >&2
    exit 1
fi

platform="linux/$arch"

# If $MULTIARCH is defined, this image is part of a multi-arch docker manifest. Append the
# architecture to image tags.  The manifest is created in a different step and will reference these
# -$arch tags.
if [ -n "${MULTIARCH-}" ]; then
    tag_version=$VERSION-$arch
else
    tag_version=$VERSION
fi

docker_repo="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
image="$docker_repo/$image_name"

# Pull the latest image to populate the cache layers.  This can make the container build go much
# faster.
echo "--- :docker: Pulling $image:latest"
if docker pull --quiet "$image:latest"; then
    cache_from="--cache-from=$image:latest"
else
    echo "Failed to pull previous build of image"
    cache_from=""
fi

build_cmd_prefix=(
    "docker" "buildx" "build" \
    "--platform" "$platform" \
    "--file" "$dockerfile" \
    "--tag" "$image:$tag_version" \
    "--progress" "plain" \
    "--load" \
    "--build-arg" "BUILDKIT_INLINE_CACHE=1" \
)
build_cmd_suffix=(
    "$@" \
    "$context"
)
if [ -n "$cache_from" ]; then
    build_cmd=("${build_cmd_prefix[@]}" "$cache_from" "${build_cmd_suffix[@]}")
else
    build_cmd=("${build_cmd_prefix[@]}" "${build_cmd_suffix[@]}")
fi

echo "+++ :docker: Building $image:$tag_version"

# Must initialize buildx builder for multi-arch use:
docker buildx create \
  --name container-builder \
  --driver docker-container \
  --bootstrap --use

"${build_cmd[@]}"
tags_to_push=("$image:$tag_version")

if [[ "${BUILDKITE_BRANCH:-}" == "refs/heads/main" && -z "${MULTIARCH:-}" ]]; then
    # If this is main and we're not building multi-arch image, then we need to push "latest".
    # Multi-arch images require a later manifest creation step which will be pushed as "latest".
    docker tag "$image:$tag_version" "$image:latest"
    tags_to_push+=("$image:latest")
fi

# Push the tags, unless SKIP_PUSH is defined:
if [ -z "${SKIP_PUSH+x}" ]; then
  for tag in "${tags_to_push[@]}"; do
      echo "--- :docker: Pushing $tag"
      docker push "$tag"
  done
fi
