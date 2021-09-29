#!/usr/bin/env bash
set -euo pipefail

dockerfile="$1"
image_name="$2"

if [ -z "$dockerfile" ] || [ -z "$image_name" ]; then
    echo "Usage: $0 <dockerfile> <image_name> [<docker-build-args>...]" >&2
    exit 1
fi
shift 2

AWS_ACCOUNT=${AWS_ACCOUNT:-305232526136} # build
AWS_REGION=${AWS_REGION:-us-east-2}
VERSION=${VERSION:-$BUILDKITE_COMMIT}

docker_repo="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
image="$docker_repo/$image_name"

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
    "."
)
if [ -n "$cache_from" ]; then
    build_cmd=("${build_cmd_prefix[@]}" "$cache_from" "${build_cmd_suffix[@]}")
else
    build_cmd=("${build_cmd_prefix[@]}" "${build_cmd_suffix[@]}")
fi

echo "+++ :docker: Building $image:$VERSION"
DOCKER_BUILDKIT=1 "${build_cmd[@]}"

if [ "$BUILDKITE_BRANCH" = "refs/heads/main" ]; then
    docker tag "$image:$VERSION" "$image:latest"
    docker tag "$image:$VERSION" "$image:release-${BUILDKITE_COMMIT}"
fi

echo "--- :docker: Pushing $image"
docker push --all-tags "$image"
