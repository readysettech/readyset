# Global/Managed-Services Root Module

The purpose of this root-module is to provide a central place for us to managed global services that are managed on a per-account basis. This is an ideal place to manage route53 zones, for instance, as well as non-environment specific IAM resources.

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | = 1.0.2 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 3.74.0 |
| <a name="requirement_external"></a> [external](#requirement\_external) | >= 2.2.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws.dns"></a> [aws.dns](#provider\_aws.dns) | 3.74.1 |
| <a name="provider_aws.network"></a> [aws.network](#provider\_aws.network) | 3.74.1 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [aws_route53_vpc_association_authorization.private-readyset](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/route53_vpc_association_authorization) | resource |
| [aws_route53_zone.private-readyset](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/route53_zone) | resource |
| [aws_route53_zone_association.dns-enabled-vpcs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/route53_zone_association) | resource |
| [aws_vpc.admin-blackhole-network](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/vpc) | data source |
| [aws_vpc.build-default-network](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/vpc) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_private_hosted_zone_enabled"></a> [private\_hosted\_zone\_enabled](#input\_private\_hosted\_zone\_enabled) | Toggles provisioning of the readyset\_domain\_name private hosted zone in the Admin account. | `bool` | `false` | no |
| <a name="input_private_hosted_zone_id"></a> [private\_hosted\_zone\_id](#input\_private\_hosted\_zone\_id) | If the private hosted zone already exists in the admin account, explicitly stating the zoneID here makes sense. | `string` | `""` | no |
| <a name="input_private_hosted_zone_name"></a> [private\_hosted\_zone\_name](#input\_private\_hosted\_zone\_name) | When private\_hosted\_zone\_enabled is true, this will be used to name the created private hosted zone. | `string` | `"readyset.name"` | no |

## Outputs

No outputs.
