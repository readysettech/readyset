#!/bin/bash -ex

echo "--- Pushing test image for ${1}"
image="$(./readyset-framework-testing/run.sh generate_image_name "${1}")";
docker push "${image}";
