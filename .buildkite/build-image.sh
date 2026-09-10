#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

# This script is used by Buildkite to standardize a few things about how we
# build Docker containers that are used for other steps in Buildkite.
# The script ensures the following things:
# Docker Buildkit is used for better performance and caching
# Builds are uploaded to our private AWS Elastic Container Registry
# When builds are done on main, they are tagged as latest to help with caching.

if [ $# -lt 3 ]; then
    echo "Usage: $0 <dockerfile> <image_name> <context> [<docker-build-args>...]" >&2
    exit 1
fi
dockerfile="$1"
image_name="$2"
context="$3"
shift 3

# In CI pipeline, BUILDKITE_COMMIT will be defined.  If not, retrieve from git.
BUILDKITE_COMMIT=${BUILDKITE_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo "")}
if [ -z "$BUILDKITE_COMMIT" ]; then
  echo "Error: Unable to determine git commit" >&2
  exit 1
fi

AWS_ACCOUNT=${AWS_ACCOUNT:-305232526136} # build
AWS_REGION=${AWS_REGION:-us-east-2}
VERSION=${VERSION:-$BUILDKITE_COMMIT}
RELEASE=${RELEASE:-"false"}


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

if [[ "${RELEASE}" == "true" ]]; then
    release_buildargs=("--build-arg" "RELEASE_VERSION=${RELEASE_VERSION:-}")
else
    release_buildargs=()
fi

docker_repo="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
image="$docker_repo/$image_name"

build_week=$(date +%Y%W)

# Use an already published image to populate the cache layers. We attempt to pull a candidate, then
# inspect it to verify the architecture matches before using it for cache.
#
# With EXPIRE_CACHE_WEEKLY set, a candidate stamped with an earlier week is rejected and the build
# runs with --no-cache so that images whose dockerfiles fetch unpinned dependencies will pick up current
# versions weekly.
cache_from=()
no_cache=()
function add_cache_source() {
    local ref="$1" pulled_arch pulled_week
    echo "--- :docker: Pulling $ref"
    if ! docker pull --platform "$platform" --quiet "$ref" 2>/dev/null; then
        echo "No $ref found"
        return
    fi
    pulled_arch=$(docker image inspect "$ref" --format='{{.Architecture}}' 2>/dev/null)
    if [ "$pulled_arch" != "$arch" ]; then
        echo "Warning: $ref is for $pulled_arch, not $arch (removing)"
        docker rmi "$ref" >/dev/null 2>&1
        return
    fi
    if [ -n "${EXPIRE_CACHE_WEEKLY-}" ]; then
        pulled_week=$(docker image inspect "$ref" \
            --format='{{index .Config.Labels "build.week"}}' 2>/dev/null || true)
        if [ "$pulled_week" != "$build_week" ]; then
            echo "Ignoring $ref: built in week ${pulled_week:-unknown}, not $build_week"
            no_cache=("--no-cache")
            return
        fi
    fi
    cache_from+=("--cache-from=$ref")
}

# Prefer this commit's own image, which an earlier attempt at the same commit may already have
# pushed.  Otherwise, pull latest.
add_cache_source "$image:$tag_version"
if [ ${#cache_from[@]} -eq 0 ] && [ ${#no_cache[@]} -eq 0 ]; then
    add_cache_source "$image:latest"
fi

build_cmd_prefix=(
    "docker" "buildx" "build"
    "--platform" "$platform"
    "--file" "$dockerfile"
    "--tag" "$image:$tag_version"
    "--progress" "plain"
    "--load"
    "--build-arg" "BUILDKIT_INLINE_CACHE=1"
    "--build-arg" "BUILDKITE_COMMIT=$BUILDKITE_COMMIT"
)

# Add a label with the week number so that we don't run out of ECR image tags. We are limited to
# 1000 tags per image. An "image" here means an image digest. Adding a label to an image changes the
# metadata portion of the image, which changes the digest without changing or adding any layers. We
# may have to make this more frequent if we ever hit the 1000 tag limit within a week, but that
# seems like it would be a pathological situation.
build_cmd_prefix+=("--label" "build.week=$build_week")

if [[ -n "${SCCACHE_BUCKET-}" ]] && [[ -n "${SCCACHE_REGION-}" ]]; then
    build_cmd_prefix+=(
        "--build-arg" "SCCACHE_BUCKET"
        "--build-arg" "SCCACHE_REGION"
    )
fi

if [[ -n "${TCV_IMAGE_TAG-}" ]]; then
    build_cmd_prefix+=("--build-arg" "TCV_IMAGE_TAG")
fi


build_cmd_suffix=(
    "$@"
    "$context"
)

build_cmd=("${build_cmd_prefix[@]}")

if [ ${#release_buildargs[@]} -gt 0 ]; then
    build_cmd+=("${release_buildargs[@]}")
fi

if [ ${#cache_from[@]} -gt 0 ]; then
    build_cmd+=("${cache_from[@]}")
fi

if [ ${#no_cache[@]} -gt 0 ]; then
    build_cmd+=("${no_cache[@]}")
fi

build_cmd+=("${build_cmd_suffix[@]}")

echo "+++ :docker: Building $image:$tag_version"

# Create or use existing builder
if ! docker buildx inspect container-builder >/dev/null 2>&1; then
  echo "--- :docker: Creating buildx builder"
  docker buildx create \
    --name container-builder \
    --driver docker-container \
    --bootstrap || {
      echo "Error: Failed to create buildx builder" >&2
      exit 1
    }
fi
docker buildx use container-builder || {
    echo "Error: Failed to switch to container-builder" >&2
    exit 1
}

set -x  # Log the docker build command
"${build_cmd[@]}"
set +x

tags_to_push=("$image:$tag_version")

if [[ "${BUILDKITE_BRANCH:-}" == "refs/heads/main" && -z "${MULTIARCH:-}" ]]; then
    # If this is main and we're not building multi-arch image, then we need to push "latest".
    # Multi-arch images require a later manifest creation step which will be pushed as "latest".
    docker tag "$image:$tag_version" "$image:latest"
    tags_to_push+=("$image:latest")
fi

# Push the tags, unless SKIP_PUSH is defined:
if [ -z "${SKIP_PUSH-}" ]; then
  for tag in "${tags_to_push[@]}"; do
      echo "--- :docker: Pushing $tag"
      docker push "$tag"
  done
fi
