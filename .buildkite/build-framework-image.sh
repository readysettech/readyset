#!/bin/bash -ex

echo "--- Building test image for ${1}"
./readyset-framework-testing/run.sh build_image "${1}";
image="$(./readyset-framework-testing/run.sh generate_image_name "${1}")";
mkdir -pv ".buildkite/image/$(dirname "${1}")";
docker save "${image}" | gzip > ".buildkite/image/${1}.tar.gz";
buildkite-agent artifact upload ".buildkite/image/${1}.tar.gz";
