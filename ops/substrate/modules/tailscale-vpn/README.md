# Tailscale VPN Module

The purpose of this module is to deploy a Tailscale subnet router within the specified VPC. Packer is used to build the AMI needed by this module. You may find the Packer definitions for this AMI [here](https://gerrit.readyset.name/plugins/gitiles/readyset/+/refs/heads/main/ops/images/build-tailscale-subnet-router.pkr.hcl).

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| aws | n/a |
| template | n/a |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| ami\_id | AMI to use for Tailscale subnet router EC2 instances. | `string` | n/a | yes |
| aws\_region | The AWS region to create resources in. | `any` | n/a | yes |
| enable\_detailed\_monitoring | Toggles CloudWatch detailed monitoring of Tailscale subnet router EC2 instance. | `bool` | `false` | no |
| environment | The name of the Substrate environment. | `string` | n/a | yes |
| iam\_authorized\_secrets\_manager\_arn | Secrets Manager resource ARN to authorize Tailscale subnet router IAM role to read. | `string` | n/a | yes |
| iam\_authorized\_secrets\_manager\_kms\_key\_arn | ARN of the KMS key to be used when decrypting Secrets Manager secrets. | `string` | n/a | yes |
| instance\_type | Instance type to apply to Tailscale subnet router EC2s. | `string` | `"t3.micro"` | no |
| key\_pair\_name | The EC2 key pair to assign to the created Tailscale subnet router instance. | `string` | n/a | yes |
| quality | The name of the Substrate quality to label this deployment with. | `string` | n/a | yes |
| resource\_tags | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |
| root\_volume\_configs | Configuration object for root volumes associated with Tailscale subnet router EC2 instance. | `object({ volume_size : number, delete_on_termination : bool })` | n/a | yes |
| ssh\_access\_enabled | Toggles security group rule to allow inbound SSH traffic from ssh\_allowed\_cidrs | `bool` | `false` | no |
| ssh\_allowed\_ingress\_cidrs | List of CIDR blocks to authorize ingress ssh traffic from. | `list(string)` | <pre>[<br>  "10.0.0.0/18"<br>]</pre> | no |
| ssh\_port\_number | Port number that SSHd is listening on. Used to grant ingress rules on Tailscale security group. | `number` | `22` | no |
| ts\_cfg\_advertised\_routes | CIDR ranges for Tailscale to advertise to SaaS control plane. | `list(string)` | `[]` | no |
| vpc\_id | ID of the VPC to deploy Tailscale subnet routers within. | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| eip\_object | Elastic IP object associated with the Tailscale subnet router instance. |
| iam\_instance\_profile\_arn | ARN of the IAM instance profile created for the Tailscale subnet router instance. |
| iam\_instance\_profile\_name | Name of the IAM instance profile created for the Tailscale subnet router instance. |
| iam\_role\_arn | ARN of the IAM role created for the Tailscale subnet router instance. |
| iam\_role\_name | Name of the IAM role created for the Tailscale subnet router instance. |
| vpc\_id | ID of the VPC to deploy Tailscale subnet router within. |
| vpc\_security\_group\_id | ID of the VPC security group applied to subnet router instances. |
