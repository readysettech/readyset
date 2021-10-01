#!/bin/bash
set -euo pipefail

release_name="$(buildkite-agent meta-data get release-name)"
# TODO Add validation of release names

ecr_repository_namespace="305232526136.dkr.ecr.us-east-2.amazonaws.com"
images=(
  readyset-server
  readyset-mysql
  readyset-psql
)

# The step uploading to S3 adds in a to the key to avoid rewriting a release by
# accident. Commands to deliver to others will find the most recent version
# by default.
# TODO: Make bucket for releases actually immutable in some way.

echo "steps:"

for image in "${images[@]}"; do
cat << EOF
  - name: ":docker: Create Docker archive for $image"
    key: docker-archive-$image
    commands:
      - docker pull ${ecr_repository_namespace}/${image}:release-${BUILDKITE_COMMIT}
      - docker tag ${ecr_repository_namespace}/${image}:release-${BUILDKITE_COMMIT} ${image}:${release_name}
      - docker save ${image}:${release_name} | gzip > ${image}-${release_name}.tar.gz
      - buildkite-agent artifact upload ${image}-${release_name}.tar.gz
    plugins:
      ecr#v2.5.0:
        login: true
        retries: 3

  - name: ":amazon-s3: Upload Docker archive for $image to S3"
    depends_on:
      - docker-archive-$image
    key: s3-upload-$image
    commands:
      - buildkite-agent artifact download ${image}-${release_name}.tar.gz .
      - >-
        aws s3 cp ${image}-${release_name}.tar.gz
        s3://readysettech-customer-artifacts-us-east-2/docker-release-${release_name}/$(date --rfc-3339=date --utc)/${image}.tar.gz
    plugins:
    env:
      AWS_ASSUME_ROLE_ARN: arn:aws:iam::305232526136:role/DeployCustomerArtifactsWrite
    plugins:
      - cultureamp/aws-assume-role#v0.2.0
EOF
done
