#!/bin/sh
set -eux

# Set up a CFN stack, run basic sanity checks, and tear it down.
# These steps *must* be broken out into a script since buildkite does not play nicely with shell variable capture

cd ops/cfn
mkdir ./rendered_templates
buildkite-agent artifact download "*.yaml" --step packer-cfn-template-ytt ./rendered_templates
cd ./rendered_templates
# cfncitestkey is provisioned and maintained by hand at the moment
# We only capture the last line of script output since that's the only line that will contain the stack name.
test_stack=$(../../cfn-test/create-mysql-super-stack.sh -h ${BUILDKITE_COMMIT} -k cfncitestkey | tail -1)
aws --region us-east-2 cloudformation delete-stack --stack-name "$test_stack"