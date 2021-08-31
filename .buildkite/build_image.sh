#!/usr/bin/env bash
set -xeuo pipefail

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
docker pull "$image:latest" || echo "Failed to pull previous build of image"

echo "+++ :docker: Building $image:$VERSION"
docker build \
    -f "$dockerfile" \
    --cache-from "$image:latest" \
    -t "$image:$VERSION" \
    "$@" \
    .

if [ "$BUILDKITE_BRANCH" = "refs/heads/main" ]; then
    docker tag "$image:$VERSION" "$image:latest"
fi

echo "--- :docker: Pushing $image"
docker push --all-tags "$image"
