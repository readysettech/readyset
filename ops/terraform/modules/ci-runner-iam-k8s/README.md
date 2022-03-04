# CI-Runner-IAM-K8s

A Terraform module that encapsulates AWS IAM resources, as well as k8s resource to facilitate providing a particular role with the ability to perform actions inside of the destination Kubernetes cluster, such as scheduling pods.

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | n/a |
| <a name="provider_kubernetes"></a> [kubernetes](#provider\_kubernetes) | n/a |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_cfn-iam-role"></a> [cfn-iam-role](#module\_cfn-iam-role) | terraform-aws-modules/iam/aws//modules/iam-assumable-role | 4.7.0 |

## Resources

| Name | Type |
|------|------|
| [aws_iam_policy.assume-role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.this-cfn](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [kubernetes_role.this](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role) | resource |
| [kubernetes_role_binding.this-rb](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role_binding) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_iam_policy_document.cfn-actions](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.grant-assume-role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment. | `string` | n/a | yes |
| <a name="input_iam_role_trusted_account_ids"></a> [iam\_role\_trusted\_account\_ids](#input\_iam\_role\_trusted\_account\_ids) | AWS accounts to permit assumption of the created IAM role. Should be empty if not using this role to traverse account boundaries. | `list(string)` | `[]` | no |
| <a name="input_k8s_role_grants"></a> [k8s\_role\_grants](#input\_k8s\_role\_grants) | Object representing k8s RBAC rules to be applied to the k8s role. | `map(any)` | <pre>{<br>  "build": [<br>    {<br>      "api_groups": [<br>        "apps",<br>        "batch",<br>        "extensions"<br>      ],<br>      "resources": [<br>        "configmaps",<br>        "cronjobs",<br>        "jobs",<br>        "pods",<br>        "services"<br>      ],<br>      "verbs": [<br>        "create",<br>        "describe",<br>        "get",<br>        "watch",<br>        "list",<br>        "delete"<br>      ]<br>    }<br>  ]<br>}</pre> | no |
| <a name="input_k8s_role_name"></a> [k8s\_role\_name](#input\_k8s\_role\_name) | Name of Kubernetes RBAC role to be created for mapping to the IAM role. | `string` | `"ci-runner"` | no |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_iam_role_arn"></a> [iam\_role\_arn](#output\_iam\_role\_arn) | ARN of the IAM role created by the module. |
| <a name="output_iam_role_name"></a> [iam\_role\_name](#output\_iam\_role\_name) | Name of the IAM role created by the module. |
