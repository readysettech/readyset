## Cluster-Autoscaler Terraform Module for EKS

The purpose of this module is to provide an easy way to deploy the Cluster-Autoscaler Helm chart into a given Kubernetes cluster and AWS account. AWS IAM components are also encapsulated in this module to isolate permissions between clusters.

For more information about Cluster-Autoscaler, check out the [docs here](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler).

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | n/a |
| <a name="provider_helm"></a> [helm](#provider\_helm) | n/a |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_iam_assumable_role_cluster_autoscaler"></a> [iam\_assumable\_role\_cluster\_autoscaler](#module\_iam\_assumable\_role\_cluster\_autoscaler) | terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc | ~> 4.0 |

## Resources

| Name | Type |
|------|------|
| [aws_iam_policy.cluster_autoscaler](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [helm_release.cluster_autoscaler](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |
| [aws_iam_policy_document.cluster_autoscaler](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_cluster_autoscaler_expander_mode"></a> [cluster\_autoscaler\_expander\_mode](#input\_cluster\_autoscaler\_expander\_mode) | Expander mode for Cluster Autoscaler to operate in. See docs: https://bit.ly/3soH1T0 | `string` | `"least-waste"` | no |
| <a name="input_cluster_id"></a> [cluster\_id](#input\_cluster\_id) | ID of the EKS cluster to be augmented with cluster autoscaling. | `any` | n/a | yes |
| <a name="input_cluster_oidc_issuer_url"></a> [cluster\_oidc\_issuer\_url](#input\_cluster\_oidc\_issuer\_url) | OIDC issuer URL for the EKS cluster. | `any` | n/a | yes |
| <a name="input_helm_chart_name"></a> [helm\_chart\_name](#input\_helm\_chart\_name) | Name of the Cluster Autoscaler chart in helm\_chart\_repository. | `string` | `"cluster-autoscaler"` | no |
| <a name="input_helm_chart_repository"></a> [helm\_chart\_repository](#input\_helm\_chart\_repository) | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://kubernetes.github.io/autoscaler"` | no |
| <a name="input_helm_chart_version"></a> [helm\_chart\_version](#input\_helm\_chart\_version) | Version of Cluster Autoscaler Helm Chart to deploy. | `string` | `"9.12.0"` | no |
| <a name="input_helm_deployment_name"></a> [helm\_deployment\_name](#input\_helm\_deployment\_name) | Name of the Cluster Autoscaler Helm release to create. | `string` | `"cluster-autoscaler"` | no |
| <a name="input_helm_deployment_namespace"></a> [helm\_deployment\_namespace](#input\_helm\_deployment\_namespace) | Namespace to deploy the Cluster Autoscaler Helm chart into. | `string` | `"kube-system"` | no |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |

## Outputs

No outputs.
