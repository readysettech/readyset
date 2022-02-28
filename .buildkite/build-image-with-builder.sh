#!/usr/bin/env bash
set -euxo pipefail

# This script is used by Buildkite to standardize a few things about how we
# build Docker containers that contain Readyset code.
# The script ensures the following things:
# Docker Buildkit is used for better performance and caching
# Environment variables to help with compile caching are set appropriately
# Builds are uploaded to our private AWS Elastic Container Registry
# When builds are done on main, they are tagged as latest to help with caching.

dockerfile="$1"
image_name="$2"
context="$3"

if [ -z "$dockerfile" ] || [ -z "$image_name" ]; then
    echo "Usage: $0 <dockerfile> <tag> <context> [<docker-build-args>...]" >&2
    exit 1
fi
shift 3

BUILDKITE_BRANCH=${BUILDKITE_BRANCH:-}
BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-}
AWS_ACCOUNT=${AWS_ACCOUNT:-305232526136} # build
AWS_REGION=${AWS_REGION:-us-east-2}

CACHEPOT_BUCKET=${CACHEPOT_BUCKET:-readysettech-build-sccache-us-east-2}
CACHEPOT_REGION=${CACHEPOT_REGION:-${AWS_REGION}}

RELEASE=${RELEASE:-"false"}
VERSION=${VERSION:-$BUILDKITE_COMMIT}
BUILDER_VERSION=${BUILDER_VERSION:-$VERSION}

docker_repo="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
image="$docker_repo/$image_name"

if [[ "${RELEASE}" == "true" ]]; then
    release_buildargs=("--build-arg" "release=1")
    tag="release-${VERSION}"
else
    release_buildargs=()
    tag="${VERSION}"
fi

echo "--- :docker: Pulling $image:latest"
if docker pull "$image:latest"; then
    cache_from="--cache-from=$image:latest"
else
    echo "Failed to pull previous build of image"
    cache_from=""
fi

build_cmd_prefix=(
    "docker" "build" \
    "-f" "$dockerfile" \
    "-t" "$image:$tag" \
    "--build-arg" "BUILDKIT_INLINE_CACHE=1" \
    "--build-arg" "CACHEPOT_BUCKET"
    "--build-arg" "CACHEPOT_REGION"
    "--build-arg" "READYSET_RUST_UBUNTU2004_BUILDER_BASE_TAG=${BUILDER_VERSION}"
)

build_cmd_suffix=(
    "$@" \
    "$context"
)
if [ -n "$cache_from" ] &&  [ -n "${release_buildargs[*]-}" ]; then
    build_cmd=("${build_cmd_prefix[@]}" "${release_buildargs[@]}" "$cache_from" "${build_cmd_suffix[@]}")
elif [ -n "$cache_from" ]; then
    build_cmd=("${build_cmd_prefix[@]}" "$cache_from" "${build_cmd_suffix[@]}")
elif [ -n "${release_buildargs[*]-}" ]; then
    build_cmd=("${build_cmd_prefix[@]}" "${release_buildargs[@]}" "${build_cmd_suffix[@]}")
else
    build_cmd=("${build_cmd_prefix[@]}" "${build_cmd_suffix[@]}")
fi

echo "+++ :docker: Building $image:$VERSION"
DOCKER_BUILDKIT=1 "${build_cmd[@]}"
tags_to_push=("$image:$tag")

if [[ "$BUILDKITE_BRANCH" == "refs/heads/main" ]]; then
    if [[ "${RELEASE}" == "true" ]]; then
        latest_tag="release-latest"
    else
        latest_tag="latest"
    fi
    docker tag "$image:$tag" "$image:$latest_tag"
    tags_to_push+=("$image:$latest_tag")
fi

for tag in "${tags_to_push[@]}"; do
    echo "--- :docker: Pushing $tag"
    docker push "$tag"
done
