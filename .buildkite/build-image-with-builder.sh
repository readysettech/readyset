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
        set +eo pipefail

        manifest=$(
            aws ecr batch-get-image \
                --repository-name "$image_name" \
                --image-ids imageTag=latest \
                --output json \
                | jq -r '.images[0].imageManifest')

        # This might fail if the image already exists (eg if we're
        # rebuilding the same commit), and we don't want to fail if that's
        # the case - if the operation fails for *another* reason, we'll just
        # fail in a later build because the image doesn't exist
        aws ecr put-image \
            --repository-name "$image_name" \
            --image-tag "$BUILDKITE_COMMIT" \
            --image-manifest "$manifest" || true \
            > /dev/null
        exit 0
    fi
fi

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
    # Always push image for commit
    docker tag "$image:$tag" "$image:$VERSION"
    tags_to_push+=("$image:$VERSION")
fi

for tag in "${tags_to_push[@]}"; do
    echo "--- :docker: Pushing $tag"
    docker push "$tag"
done
