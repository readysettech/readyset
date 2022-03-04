# ReadySet Supporting Infra for US-EAST-2 in Build AWS Account

This is the Terraform/Substrate root module by which AWS resources for builds and other generic product-focused infrastructure is provisioned.

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | = 1.0.2 |
| <a name="requirement_archive"></a> [archive](#requirement\_archive) | >= 2.2.0 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 3.49.0 |
| <a name="requirement_external"></a> [external](#requirement\_external) | >= 2.1.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | 3.60.0 |
| <a name="provider_aws.network"></a> [aws.network](#provider\_aws.network) | 3.60.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_buildkite_benchmark_queue"></a> [buildkite\_benchmark\_queue](#module\_buildkite\_benchmark\_queue) | ../../../../../modules/buildkite-queue/regional | n/a |
| <a name="module_buildkite_default_queue"></a> [buildkite\_default\_queue](#module\_buildkite\_default\_queue) | ../../../../../modules/buildkite-queue/regional | n/a |
| <a name="module_buildkite_k8s_queue"></a> [buildkite\_k8s\_queue](#module\_buildkite\_k8s\_queue) | ../../../../../modules/buildkite-queue/regional | n/a |
| <a name="module_buildkite_ops_queue"></a> [buildkite\_ops\_queue](#module\_buildkite\_ops\_queue) | ../../../../../modules/buildkite-queue/regional | n/a |
| <a name="module_buildkite_queue"></a> [buildkite\_queue](#module\_buildkite\_queue) | ../../../../../modules/buildkite-queue/regional | n/a |
| <a name="module_buildkite_queue_shared"></a> [buildkite\_queue\_shared](#module\_buildkite\_queue\_shared) | ../../../../../modules/buildkite-queue-shared/regional | n/a |
| <a name="module_ci-benchmarking-iam-role"></a> [ci-benchmarking-iam-role](#module\_ci-benchmarking-iam-role) | terraform-aws-modules/iam/aws//modules/iam-assumable-role | 4.7.0 |
| <a name="module_readyset"></a> [readyset](#module\_readyset) | ../../../../../modules/build/regional | n/a |

## Resources

| Name | Type |
|------|------|
| [aws_ec2_tag.readyset-build-default-subnet-connectivity-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ec2_tag) | resource |
| [aws_ec2_tag.readyset-build-default-subnet-environment-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ec2_tag) | resource |
| [aws_ec2_tag.readyset-build-default-subnet-name-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ec2_tag) | resource |
| [aws_ec2_tag.readyset-build-default-subnet-quality-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ec2_tag) | resource |
| [aws_ec2_tag.readyset-build-default-vpc-environment-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ec2_tag) | resource |
| [aws_ec2_tag.readyset-build-default-vpc-name-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ec2_tag) | resource |
| [aws_ec2_tag.readyset-build-default-vpc-quality-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ec2_tag) | resource |
| [aws_iam_policy.bk-benchmarking-assume-role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.bk-k8s-assume-role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.ci-benchmarking](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_instance.docs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/instance) | resource |
| [aws_ram_principal_association.readyset-build-default-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ram_principal_association) | resource |
| [aws_ram_resource_association.readyset-build-default-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ram_resource_association) | resource |
| [aws_ram_resource_share.readyset-build-default-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ram_resource_share) | resource |
| [aws_s3_bucket.devops-assets](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket) | resource |
| [aws_s3_bucket.ops-artifacts](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket) | resource |
| [aws_s3_bucket.ops-secrets](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket) | resource |
| [aws_s3_bucket.sccache](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket) | resource |
| [aws_s3_bucket_public_access_block.devops-assets](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_public_access_block) | resource |
| [aws_s3_bucket_public_access_block.ops-artifacts](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_public_access_block) | resource |
| [aws_s3_bucket_public_access_block.ops-secrets](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_public_access_block) | resource |
| [aws_s3_bucket_public_access_block.sccache](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_public_access_block) | resource |
| [aws_security_group.docs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group) | resource |
| [aws_security_group_rule.docs_all_egress](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.docs_ssh_from_vpc](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_iam_policy_document.bk-benchmarking-assume-role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.bk-k8s-assume-role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.ci-benchmarking](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_kms_key.s3-default](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/kms_key) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |
| [aws_subnet.docs_subnet](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet) | data source |
| [aws_subnet.readyset-build-default-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet) | data source |
| [aws_subnet_ids.readyset-build-default-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet_ids) | data source |
| [aws_vpc.docs_vpc](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/vpc) | data source |
| [aws_vpc.readyset-build-default-us-east-2](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/vpc) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_benchmarking_iam_role_enabled"></a> [benchmarking\_iam\_role\_enabled](#input\_benchmarking\_iam\_role\_enabled) | Toggles creation of AWS IAM resources required for Benchmarking. | `bool` | `false` | no |
| <a name="input_benchmarking_iam_role_trusted_account_ids"></a> [benchmarking\_iam\_role\_trusted\_account\_ids](#input\_benchmarking\_iam\_role\_trusted\_account\_ids) | AWS accounts to permit assumption of Benchmarking IAM role. Should be empty if not using this role xaccount boundaries. | `list(string)` | `[]` | no |
| <a name="input_buildkite_k8s_queue_iam_role_arns"></a> [buildkite\_k8s\_queue\_iam\_role\_arns](#input\_buildkite\_k8s\_queue\_iam\_role\_arns) | ARN of additional IAM roles to grant Buildkite agents in the buildk8s queue permission to assume. | `list(string)` | <pre>[<br>  "arn:aws:iam::305232526136:role/readyset-ci-k8s-build-us-east-2"<br>]</pre> | no |
| <a name="input_buildkite_k8s_queue_iam_role_enabled"></a> [buildkite\_k8s\_queue\_iam\_role\_enabled](#input\_buildkite\_k8s\_queue\_iam\_role\_enabled) | Toggles creation of AWS IAM resources required for Benchmarking agents in the buildk8s queue. | `bool` | `true` | no |
| <a name="input_devops_assets_s3_bucket_enabled"></a> [devops\_assets\_s3\_bucket\_enabled](#input\_devops\_assets\_s3\_bucket\_enabled) | Toggles creation of s3 bucket containing devops assets used during builds or benchmarking. | `bool` | `false` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the Substrate environment. | `string` | n/a | yes |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to resources. | `map(any)` | <pre>{<br>  "managed": "terraform"<br>}</pre> | no |

## Outputs

No outputs.
