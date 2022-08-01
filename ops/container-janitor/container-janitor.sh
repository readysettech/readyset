#!/bin/bash

########################
# container-janitor.sh
#
# Script to scan all repositories that are NOT mirrors and output:
# <repository name>: <total count of images> -- <sha:digest of container>: <max_by count of tags for sha:digest>
# ex. readyset-server: 2000 -- sha256:f6e55178886b3b752b5dba5ff2b24b47b6c83632e58451793db6db724de807e0: 3
#
# Must have already minted AWS creds with substrate. Approximate runtime of 5 minutes.
########################
set -euo pipefail

SUBSTRATE_CMD="substrate assume-role -domain readyset -environment build -quality default"
REPOSITORIES=$(${SUBSTRATE_CMD} aws ecr describe-repositories | \
	jq -r '.repositories | .[] |
	select(all(.repositoryName; contains("mirror") | not)) | .repositoryName')

for repository in $REPOSITORIES; do
	IMAGES_COUNT=$(${SUBSTRATE_CMD} aws ecr describe-images --repository-name "${repository}" | \
		jq -r '.imageDetails | length')
	TAG_COUNT=$(${SUBSTRATE_CMD} aws ecr describe-images --repository-name "${repository}" | \
		jq -r '[.imageDetails[] | {digest: .imageDigest, tag_count: (.imageTags | length)}]
		| max_by(.tag_count) | "\(.digest): \(.tag_count)"')

	echo "${repository}: ${IMAGES_COUNT} -- ${TAG_COUNT}"
done
