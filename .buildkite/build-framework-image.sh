#!/bin/bash -ex

dialect="${1}"
framework="${2}"

echo "--- Building test image for ${framework} on ${dialect}"
./readyset-framework-testing/run.sh build_image "${dialect}" "${framework}";
image="$(./readyset-framework-testing/run.sh generate_image_name "${dialect}" "${framework}")";
mkdir -pv ".buildkite/image/$(dirname "${framework}")";
docker save "${image}" | gzip > ".buildkite/image/${framework}.${dialect}.tar.gz";
buildkite-agent artifact upload ".buildkite/image/${framework}.${dialect}.tar.gz";
