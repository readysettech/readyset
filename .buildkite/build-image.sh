#!/usr/bin/env bash
set -euo pipefail

# This script is used by Buildkite to standardize a few things about how we
# build Docker containers that are used for other steps in Buildkite.
# The script ensures the following things:
# Docker Buildkit is used for better performance and caching
# Builds are uploaded to our private AWS Elastic Container Registry
# When builds are done on main, they are tagged as latest to help with caching.
#
# To optimize rebuilds of images that rarely change, set the SKIP_IF_UNCHANGED
# environment variable to a comma-separated lists of repository-relative paths
# to files - if none of these files have changed, the image won't actually be
# built, it'll just be re-tagged with the current commit via the AWS API.


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

docker_repo="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
image="$docker_repo/$image_name"

if [ -n "${SKIP_IF_UNCHANGED+1}" ]; then
    do_skip="1"
    changed_files="$(git diff --name-only HEAD~ HEAD)"
    IFS=',' read -ra files <<< "$SKIP_IF_UNCHANGED,$dockerfile"
    for fname in "${files[@]}"; do
        if grep "$fname" <<< "$changed_files" > /dev/null; then
            do_skip=""
        fi
    done

    if [ -n "$do_skip" ]; then
        echo "+++ :docker: Dockerfile unchanged, retagging latest image as ${BUILDKITE_COMMIT}"
        manifest=$(
            aws ecr batch-get-image \
                --repository-name "$image_name" \
                --image-ids imageTag=latest \
                --output json \
                | jq -r '.images[0].imageManifest')
        aws ecr put-image \
            --repository-name "$image_name" \
            --image-tag "$BUILDKITE_COMMIT" \
            --image-manifest "$manifest" \
            > /dev/null
        exit 0
    fi
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
    "-t" "$image:$VERSION" \
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

echo "+++ :docker: Building $image:$VERSION"
DOCKER_BUILDKIT=1 "${build_cmd[@]}"
tags_to_push=("$image:$VERSION")

if [ "$BUILDKITE_BRANCH" = "refs/heads/main" ]; then
    docker tag "$image:$VERSION" "$image:latest"
    tags_to_push+=("$image:latest")
    docker tag "$image:$VERSION" "$image:release-${BUILDKITE_COMMIT}"
    tags_to_push+=("$image:release-${BUILDKITE_COMMIT}")
fi

for tag in "${tags_to_push[@]}"; do
    echo "--- :docker: Pushing $tag"
    docker push "$tag"
done
