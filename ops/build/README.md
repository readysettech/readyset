# Readyset Ops Images

This contains two different Dockerfiles installing the tools needed for
operations interally and externally.

# Internal Ops Image

This container contains all tools needed to work in an operational capacity at
Readyset.

Tools contained:
 - Packer
 - Terraform
 - tflint
 - tflint-ruleset-aws
 - Shellcheck
 - Docker

# Cloudformation Image

This contains all the tools needed to work with AWS Cloudformation templates.

Tools contained:
  - cfn-lint
  - taskcat
