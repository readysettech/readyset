# External-DNS Terraform Module

The purpose of this Terraform module is to deploy the ExternalDNS Helm chart into the provided Kubernetes cluster. The high-level goal is to provide dynamic DNS registration as Ingress rules are modified in Kubernetes.

For more information on ExternalDNS, check out the [ExternalDNS docs](https://github.com/kubernetes-sigs/external-dns).

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| aws | n/a |
| helm | n/a |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| cluster\_id | The cluster ID for the EKS cluster. | `string` | n/a | yes |
| dns\_zone\_mode | The zone type for the module (public/private). | `string` | `"public"` | no |
| helm\_chart\_name | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"external-dns"` | no |
| helm\_chart\_repository | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://charts.bitnami.com/bitnami"` | no |
| helm\_chart\_version | Version of Helm Chart to deploy. | `string` | `"2.20.3"` | no |
| helm\_deployment\_namespace | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| oidc\_issuer\_url | The OIDC issuer URL for the EKS cluster. | `string` | n/a | yes |
| oidc\_provider\_arn | The OIDC provider ARN for the EKS cluster. | `string` | n/a | yes |
| r53\_domain | The Route53 DNS domain ExternalDNS will be using. | `string` | n/a | yes |

## Outputs

No output.
