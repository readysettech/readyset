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

echo "steps:"

for image in "${images[@]}"; do
cat << EOF
  - name: ":docker: Create Docker archive for $image"
    commands:
      - docker pull ${ecr_repository_namespace}/${image}:release-${BUILDKITE_COMMIT}
      - docker tag ${ecr_repository_namespace}/${image}:release-${BUILDKITE_COMMIT} ${image}:${release_name}
      - docker save ${image}:${release_name} | gzip > ${image}-${release_name}.tar.gz
      - buildkite-agent artifact upload ${image}-${release_name}.tar.gz
    plugins:
      ecr#v2.2.0:
        login: true
EOF
done

# TODO: Create another step which uploads these artifacts to an S3 bucket we can share.
