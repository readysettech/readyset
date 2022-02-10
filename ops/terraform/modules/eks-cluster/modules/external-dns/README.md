# External-DNS Terraform Module

The purpose of this Terraform module is to deploy the ExternalDNS Helm chart into the provided Kubernetes cluster. The high-level goal is to provide dynamic DNS registration as Ingress rules are modified in Kubernetes.

For more information on ExternalDNS, check out the [ExternalDNS docs](https://github.com/kubernetes-sigs/external-dns).

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 3.72.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 3.72.0 |
| <a name="provider_aws.dns"></a> [aws.dns](#provider\_aws.dns) | >= 3.72.0 |
| <a name="provider_helm"></a> [helm](#provider\_helm) | n/a |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_ext-dns-cross-account-zone-role"></a> [ext-dns-cross-account-zone-role](#module\_ext-dns-cross-account-zone-role) | terraform-aws-modules/iam/aws//modules/iam-assumable-role | ~> 4 |

## Resources

| Name | Type |
|------|------|
| [aws_iam_policy.externaldns-xaccount](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_role.externaldns](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role_policy.externaldns](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [helm_release.externaldns](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_caller_identity.current-dns](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_eks_cluster_auth.eks](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/eks_cluster_auth) | data source |
| [aws_iam_policy_document.externaldns](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.externaldns-policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.xaccount-zone](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |
| [aws_route53_zone.zone](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/route53_zone) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_cluster_id"></a> [cluster\_id](#input\_cluster\_id) | The cluster ID for the EKS cluster. | `string` | n/a | yes |
| <a name="input_dns_zone_mode"></a> [dns\_zone\_mode](#input\_dns\_zone\_mode) | The zone type for the module (public/private). | `string` | `"public"` | no |
| <a name="input_external_dns_cross_account_zone"></a> [external\_dns\_cross\_account\_zone](#input\_external\_dns\_cross\_account\_zone) | If enabled, toggles creation of IAM resources in secondary account indicated by provider aws.dns. | `bool` | `true` | no |
| <a name="input_helm_chart_name"></a> [helm\_chart\_name](#input\_helm\_chart\_name) | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"external-dns"` | no |
| <a name="input_helm_chart_repository"></a> [helm\_chart\_repository](#input\_helm\_chart\_repository) | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://charts.bitnami.com/bitnami"` | no |
| <a name="input_helm_chart_version"></a> [helm\_chart\_version](#input\_helm\_chart\_version) | Version of Helm Chart to deploy. | `string` | `"6.1.4"` | no |
| <a name="input_helm_deployment_namespace"></a> [helm\_deployment\_namespace](#input\_helm\_deployment\_namespace) | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| <a name="input_oidc_issuer_url"></a> [oidc\_issuer\_url](#input\_oidc\_issuer\_url) | The OIDC issuer URL for the EKS cluster. | `string` | n/a | yes |
| <a name="input_oidc_provider_arn"></a> [oidc\_provider\_arn](#input\_oidc\_provider\_arn) | The OIDC provider ARN for the EKS cluster. | `string` | n/a | yes |
| <a name="input_r53_domain"></a> [r53\_domain](#input\_r53\_domain) | The Route53 DNS domain ExternalDNS will be using. | `string` | n/a | yes |

## Outputs

No outputs.
