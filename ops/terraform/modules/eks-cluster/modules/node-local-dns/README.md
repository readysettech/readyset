## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_helm"></a> [helm](#provider\_helm) | n/a |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [helm_release.this](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_helm_deployment_name"></a> [helm\_deployment\_name](#input\_helm\_deployment\_name) | Name of the Helm release to create. | `string` | `"node-local-dns"` | no |
| <a name="input_helm_deployment_namespace"></a> [helm\_deployment\_namespace](#input\_helm\_deployment\_namespace) | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| <a name="input_vpc_dns_resolver_ip"></a> [vpc\_dns\_resolver\_ip](#input\_vpc\_dns\_resolver\_ip) | Private IP of the VPC DNS resolver which is hosting the k8s cluster. | `string` | `""` | no |

## Outputs

No outputs.
