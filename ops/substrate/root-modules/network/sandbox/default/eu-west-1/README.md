# Networking for EU-WEST-1

This is the Terraform/Substrate root module by which VPC components and ancillary networking components, such as Tailscale are provisioned.

## Requirements

| Name | Version |
|------|---------|
| terraform | = 1.0.2 |
| archive | >= 2.2.0 |
| aws | >= 3.49.0 |
| external | >= 2.1.0 |

## Providers

| Name | Version |
|------|---------|
| aws | >= 3.49.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| aws\_region | The AWS region to create resources in. | `string` | n/a | yes |
| environment | The name of the Substrate environment. | `string` | n/a | yes |
| quality | The name of the Substrate quality to label this deployment with. | `string` | `"default"` | no |
| resource\_tags | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |
| tailscale\_ami\_id | AMI ID to use when creating Tailscale Subnet Router EC2 instance(s). | `string` | n/a | yes |
| tailscale\_auth\_key\_secretsmanager\_arn | ARN of Secrets Manager secret within this region to authorize Tailscale subnet router instance role access to. | `string` | n/a | yes |
| tailscale\_enabled | Toggles creation of Tailscale subnet router module's resources. | `bool` | `true` | no |
| tailscale\_instance\_type | Instance type to apply to Tailscale subnet router EC2s. | `string` | `"t3.micro"` | no |
| tailscale\_keypair\_name | Name of the EC2 key pair to apply to the Tailscale subnet router instances. | `string` | `"readyset-devops"` | no |
| tailscale\_root\_volume\_del\_on\_term | Toggles deletion of root volume after Tailscale subnet router nodes are terminated. | `bool` | `true` | no |
| tailscale\_root\_volume\_size | Size in GB for Root EBS volume of Tailscale subnet router instances. | `number` | `30` | no |
| tailscale\_secretsmanager\_kms\_key\_arn | ARN of KMS key used for at-rest encryption of tailscale\_auth\_key\_secretsmanager\_arns. | `string` | `""` | no |
| tailscale\_ssh\_access\_enabled | Toggles security group rule to allow inbound SSH traffic to Tailscale subnet router instances from ssh\_allowed\_cidrs | `bool` | `false` | no |
| tailscale\_ssh\_allowed\_ingress\_cidrs | List of CIDR blocks to authorize Tailscale subnet router ingress ssh traffic from. | `list(string)` | <pre>[<br>  "10.0.0.0/8"<br>]</pre> | no |
| tailscale\_ssh\_port\_number | Port number that SSHd is listening on. Used to grant ingress rules on Tailscale security group. | `number` | `22` | no |

## Outputs

No output.
