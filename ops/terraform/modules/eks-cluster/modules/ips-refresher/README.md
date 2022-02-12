# IPS-Refresher Terraform Module

When running Kubernetes, you have two common ways of authorizing nodes in your cluster to pull from private ECR repositories:

1) Grant worker nodes themselves access to ECR via instance-level IAM policies and a role. This isn't quite ideal from a security perspective, incase the machine gets compromised.
2) Provide a Kubernetes secret reference as part of the k8s deployment manifests, which contains a docker config file that's periodically refreshed.

The latter approach is what this module aims to do, using an IAM role that's bound to a specific service account in Kubernetes.

By default, you can find the `imagePullSecret` value you need to label on a deployment in the `input_ips_projected_secret_name` module input.

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_utils"></a> [utils](#requirement\_utils) | >= 0.17.14 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | n/a |
| <a name="provider_helm"></a> [helm](#provider\_helm) | n/a |
| <a name="provider_utils"></a> [utils](#provider\_utils) | >= 0.17.14 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_irsa-ips-refresher"></a> [irsa-ips-refresher](#module\_irsa-ips-refresher) | terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc | ~> 4.0 |

## Resources

| Name | Type |
|------|------|
| [aws_iam_policy.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [helm_release.this](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |
| [aws_iam_policy_document.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [utils_deep_merge_yaml.values-file](https://registry.terraform.io/providers/cloudposse/utils/latest/docs/data-sources/deep_merge_yaml) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_authorized_ecr_resource_arns"></a> [authorized\_ecr\_resource\_arns](#input\_authorized\_ecr\_resource\_arns) | IAM resource list of ECR ARNs to grant read-access to. Supports wildcarding. | `list(string)` | <pre>[<br>  "*"<br>]</pre> | no |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_cluster_name"></a> [cluster\_name](#input\_cluster\_name) | The name of the k8s cluster being deployed into. | `string` | n/a | yes |
| <a name="input_ecr_account_id"></a> [ecr\_account\_id](#input\_ecr\_account\_id) | The AWS account id that ECR repos are in. | `string` | n/a | yes |
| <a name="input_helm_chart_name"></a> [helm\_chart\_name](#input\_helm\_chart\_name) | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"ips-refresher"` | no |
| <a name="input_helm_deployment_name"></a> [helm\_deployment\_name](#input\_helm\_deployment\_name) | Name of the Helm release to create. | `string` | `"ips-refresher"` | no |
| <a name="input_helm_deployment_namespace"></a> [helm\_deployment\_namespace](#input\_helm\_deployment\_namespace) | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| <a name="input_ips_projected_secret_name"></a> [ips\_projected\_secret\_name](#input\_ips\_projected\_secret\_name) | The name of the k8s secret to project the credentials for accessing the registry specified by ecr\_account\_id/aws\_region. | `string` | `"readyset-ips"` | no |
| <a name="input_k8s_service_account_name"></a> [k8s\_service\_account\_name](#input\_k8s\_service\_account\_name) | Name of the k8s service account to add as an authorized subject during OIDC configuration. | `string` | `"ips-refresher"` | no |
| <a name="input_oidc_issuer_url"></a> [oidc\_issuer\_url](#input\_oidc\_issuer\_url) | OIDC issuer URL for the EKS cluster. | `any` | n/a | yes |
| <a name="input_oidc_provider_arn"></a> [oidc\_provider\_arn](#input\_oidc\_provider\_arn) | The OIDC provider ARN for the EKS cluster. | `string` | n/a | yes |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |

## Outputs

No outputs.
