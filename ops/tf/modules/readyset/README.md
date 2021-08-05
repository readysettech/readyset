# Outdated!!!

This documentation is outdated. Also, this module will be moving out into another repository.

# ReadySet Terraform Module

Terraform module to deploy Readyset in AWS. It consists of three main components:

- ReadySet Adapter (Currently only MySQL is supported)
- ReadySet Server
- Zookeeper

## Usage

Example:

```hcl
provider "aws" {
  region = "us-west-2"
}

module "readyset" {
  source = "git@github.com:readysettech/readyset-terraform-aws.git"

  vpc              = "default"
  env              = "demo"
  allow_ssh        = true
  key_name         = "my-key"
  readyset_version = "50e2533"
}
```

By default it uses tags on subnets to select an appropriate private subnet to deploy resources. The tag names and values can be overriden using the `subnet_tag` name (defaults to `Connectivity`) and `private_subnet_tag` value (defaults to `private`). Alternatively a list of private subnet IDs can be provided using `private_subnet_ids`. Additional configuration can be found below.

To avoid a database password being stored in your Terraform configuration, specify the variable as a shell environment variable:

```
export TF_VAR_db_password=[my password]
```

Run Terraform to build the cluster:

```
terraform init
terraform apply
```

## Multi-region support

Setup a ReadySet cluster similarly to the example above, but use a module a descriptive module name. Only after running Terraform to build the first cluster, add the second module instance:

```hcl
provider "aws" {
  alias  = "usw2"
  region = "us-west-2"
}

provider "aws" {
  alias  = "use2"
  region = "us-east-2"
}

module "readyset-usw2" {
  source = "git@github.com:readysettech/readyset-terraform-aws.git"

  vpc              = "default"
  env              = "demo"
  allow_ssh        = true
  key_name         = "my-key"
  readyset_version = "50e2533"

  providers = {
    aws = aws.usw2
  }
}

module "readyset-use2" {
  source = "git@github.com:readysettech/readyset-terraform-aws.git"

  vpc              = "default"
  env              = "demo"
  allow_ssh        = true
  key_name         = "my-key"
  readyset_version = "50e2533"

  peer_region        = module.readyset-usw2.region
  peer_zookeeper_ips = module.readyset-usw2.zookeeper_ips

  providers = {
    aws = aws.usw2
  }
}
```

Notes:
- VPC peering or sharing need to be preconfigured.
- Peer addresses should only be provided to the second instance of the module.

## How to connect to instances?

If you provide an SSH key via the `key_name` variable, also set `allow_ssh = true` to allow SSH connections from within the VPC or define `ssh_allowed_cidr_blocks` to specify a list of CIDR blocks allowed. The username is `ubuntu`.

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 0.14.7 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 3.33.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 3.33.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [aws_autoscaling_group.main](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/autoscaling_group) | resource |
| [aws_db_instance.db](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/db_instance) | resource |
| [aws_db_option_group.db](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/db_option_group) | resource |
| [aws_db_parameter_group.db](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/db_parameter_group) | resource |
| [aws_db_subnet_group.db](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/db_subnet_group) | resource |
| [aws_ebs_volume.server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ebs_volume) | resource |
| [aws_ebs_volume.zookeeper](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ebs_volume) | resource |
| [aws_instance.server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/instance) | resource |
| [aws_instance.zookeeper](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/instance) | resource |
| [aws_launch_template.mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/launch_template) | resource |
| [aws_lb.main](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lb) | resource |
| [aws_lb_listener.mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lb_listener) | resource |
| [aws_lb_target_group.mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lb_target_group) | resource |
| [aws_network_interface.zookeeper](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/network_interface) | resource |
| [aws_security_group.db](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group) | resource |
| [aws_security_group.mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group) | resource |
| [aws_security_group.server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group) | resource |
| [aws_security_group.ssh](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group) | resource |
| [aws_security_group.zookeeper](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group) | resource |
| [aws_security_group_rule.db_ingress_mysql_cidr_blocks](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.db_ingress_mysql_mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.db_ingress_mysql_server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.mysql_adapter_ingress_mysql_cidr_blocks](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.server_ingress_leader](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.server_ingress_leader_cidr_blocks](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.server_ingress_leader_mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.server_ingress_worker](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.server_ingress_worker_cidr_blocks](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.server_ingress_worker_mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.ssh_ingress](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.ssh_ingress_vpc](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.zookeeper_ingress_client_cidr_blocks](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.zookeeper_ingress_client_mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.zookeeper_ingress_client_server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.zookeeper_ingress_leader](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.zookeeper_ingress_peer](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_volume_attachment.server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/volume_attachment) | resource |
| [aws_volume_attachment.zookeeper](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/volume_attachment) | resource |
| [aws_ami.mysql_adapter](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/ami) | data source |
| [aws_ami.server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/ami) | data source |
| [aws_ami.zookeeper](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/ami) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |
| [aws_subnet_ids.private](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet_ids) | data source |
| [aws_vpc.vpc](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/vpc) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_allow_ssh"></a> [allow\_ssh](#input\_allow\_ssh) | Allow SSH connections from within the VPC | `bool` | `false` | no |
| <a name="input_create_rds"></a> [create\_rds](#input\_create\_rds) | Create an RDS instance (requires rds\_instance\_id to be set) | `bool` | `false` | no |
| <a name="input_db_host"></a> [db\_host](#input\_db\_host) | Hostname of the database to replicate | `string` | `""` | no |
| <a name="input_db_name"></a> [db\_name](#input\_db\_name) | Name of the database to replicate | `string` | `""` | no |
| <a name="input_db_password"></a> [db\_password](#input\_db\_password) | Password for the database connection | `string` | n/a | yes |
| <a name="input_db_port"></a> [db\_port](#input\_db\_port) | Port of the database to replicate | `number` | `3306` | no |
| <a name="input_db_user"></a> [db\_user](#input\_db\_user) | User for the database connection | `string` | `"admin"` | no |
| <a name="input_deployment"></a> [deployment](#input\_deployment) | Unique identifier for the name of the ReadySet deployment | `string` | `"readyset"` | no |
| <a name="input_encrypt_rds_volume"></a> [encrypt\_rds\_volume](#input\_encrypt\_rds\_volume) | Encrypt the RDS instance volume | `bool` | `false` | no |
| <a name="input_encrypt_server_volume"></a> [encrypt\_server\_volume](#input\_encrypt\_server\_volume) | Encrypt the EBS volume used to persist server base tables | `bool` | `false` | no |
| <a name="input_encrypt_zookeeper_volume"></a> [encrypt\_zookeeper\_volume](#input\_encrypt\_zookeeper\_volume) | Encrypt the EBS volume used to persist Zookeeper state | `bool` | `false` | no |
| <a name="input_env"></a> [env](#input\_env) | Evironment name for the ReadySet deployment (i.e. dev, stage, prod, demo) | `string` | n/a | yes |
| <a name="input_extra_security_groups"></a> [extra\_security\_groups](#input\_extra\_security\_groups) | List of security groups to associate with all EC2 instances | `list(string)` | `[]` | no |
| <a name="input_key_name"></a> [key\_name](#input\_key\_name) | Name of the EC2 key pair to use when provisioning instances | `string` | n/a | yes |
| <a name="input_mysql_adapter_allowed_cidr_blocks"></a> [mysql\_adapter\_allowed\_cidr\_blocks](#input\_mysql\_adapter\_allowed\_cidr\_blocks) | List of CIDR blocks that are allowed to connect to the MySQL adapter | `list(string)` | `[]` | no |
| <a name="input_mysql_adapter_instance_count"></a> [mysql\_adapter\_instance\_count](#input\_mysql\_adapter\_instance\_count) | Number of EC2 instances in the MySQL adapter cluster | `number` | `1` | no |
| <a name="input_mysql_adapter_instance_type"></a> [mysql\_adapter\_instance\_type](#input\_mysql\_adapter\_instance\_type) | EC2 instance type of the MySQL adapter | `string` | `"t3.small"` | no |
| <a name="input_peer_region"></a> [peer\_region](#input\_peer\_region) | Peer region name | `string` | `""` | no |
| <a name="input_peer_zookeeper_ips"></a> [peer\_zookeeper\_ips](#input\_peer\_zookeeper\_ips) | List of peer region Zookeeper IPs | `list(string)` | `[]` | no |
| <a name="input_private_subnet_ids"></a> [private\_subnet\_ids](#input\_private\_subnet\_ids) | Override subnet tag use with explicit list of private subnets IDs | `list(string)` | `[]` | no |
| <a name="input_private_subnet_tag"></a> [private\_subnet\_tag](#input\_private\_subnet\_tag) | Subnet tag value for private subnets | `string` | `"private"` | no |
| <a name="input_rds_allowed_cidr_blocks"></a> [rds\_allowed\_cidr\_blocks](#input\_rds\_allowed\_cidr\_blocks) | List of CIDR blocks that are allowed to connect to the RDS instance when create\_rds = true | `list(string)` | `[]` | no |
| <a name="input_rds_engine_version"></a> [rds\_engine\_version](#input\_rds\_engine\_version) | MySQL engine version to use when create\_rds = true | `string` | `"8.0"` | no |
| <a name="input_rds_instance_type"></a> [rds\_instance\_type](#input\_rds\_instance\_type) | RDS instance type when create\_rds = true | `string` | `"db.t3.micro"` | no |
| <a name="input_rds_volume_kms_key_id"></a> [rds\_volume\_kms\_key\_id](#input\_rds\_volume\_kms\_key\_id) | ARN of the KMS key ID used to encrypt the RDS instance volume (ignored if encrypt\_rds = false) | `string` | `""` | no |
| <a name="input_readyset_version"></a> [readyset\_version](#input\_readyset\_version) | ReadySet version to deploy (Please ask for the latest version) | `string` | n/a | yes |
| <a name="input_server_allowed_cidr_blocks"></a> [server\_allowed\_cidr\_blocks](#input\_server\_allowed\_cidr\_blocks) | List of CIDR blocks that are allowed to connect to the server | `list(string)` | `[]` | no |
| <a name="input_server_instance_count"></a> [server\_instance\_count](#input\_server\_instance\_count) | Number of server instances in the cluster | `number` | `1` | no |
| <a name="input_server_instance_type"></a> [server\_instance\_type](#input\_server\_instance\_type) | EC2 instance type | `string` | `"t3.small"` | no |
| <a name="input_server_memory_bytes"></a> [server\_memory\_bytes](#input\_server\_memory\_bytes) | Amount of memory, in bytes, for partially materialized state (0 = unlimited) | `number` | `0` | no |
| <a name="input_server_quorum"></a> [server\_quorum](#input\_server\_quorum) | Number of workers required to start | `number` | `1` | no |
| <a name="input_server_shards"></a> [server\_shards](#input\_server\_shards) | Number of shards to use (0 = disable sharding) | `number` | `0` | no |
| <a name="input_server_volume_kms_key_id"></a> [server\_volume\_kms\_key\_id](#input\_server\_volume\_kms\_key\_id) | ARN of the KMS key ID used to encrypt the server volume (ignored if encrypt\_server\_volume = false) | `string` | `""` | no |
| <a name="input_server_volume_size"></a> [server\_volume\_size](#input\_server\_volume\_size) | Size of the volume (in Gigabytes) for persisting base tables | `number` | `100` | no |
| <a name="input_ssh_allowed_cidr_blocks"></a> [ssh\_allowed\_cidr\_blocks](#input\_ssh\_allowed\_cidr\_blocks) | List of CIDR blocks that are allowed to conntect to instances via SSH (overrides allow\_ssh) | `list(string)` | `[]` | no |
| <a name="input_subnet_tag"></a> [subnet\_tag](#input\_subnet\_tag) | Tag name used on subnets to define tier of connectivity | `string` | `"Connectivity"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Additional tags for resources | `map(any)` | `{}` | no |
| <a name="input_vpc"></a> [vpc](#input\_vpc) | Name of the VPC | `string` | n/a | yes |
| <a name="input_zookeeper_allowed_cidr_blocks"></a> [zookeeper\_allowed\_cidr\_blocks](#input\_zookeeper\_allowed\_cidr\_blocks) | List of CIDR blocks that are allowed to connect to Zookeeper | `list(string)` | `[]` | no |
| <a name="input_zookeeper_instance_count"></a> [zookeeper\_instance\_count](#input\_zookeeper\_instance\_count) | Number of Zookeeper instances in the cluster (odd number recommended to maintain quorum) | `number` | `1` | no |
| <a name="input_zookeeper_instance_type"></a> [zookeeper\_instance\_type](#input\_zookeeper\_instance\_type) | EC2 instance type to use for the Zookeeper instance(s) | `string` | `"t3.small"` | no |
| <a name="input_zookeeper_volume_kms_key_id"></a> [zookeeper\_volume\_kms\_key\_id](#input\_zookeeper\_volume\_kms\_key\_id) | ARN of the KMS key ID used to encrypt the Zookeeper volume (ignored if encrypt\_zookeeper\_volume = false) | `string` | `""` | no |
| <a name="input_zookeeper_volume_size"></a> [zookeeper\_volume\_size](#input\_zookeeper\_volume\_size) | Size of the volume (in Gigabytes) for persisting Zookeeper state | `number` | `100` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_region"></a> [region](#output\_region) | The region of the VPC |
| <a name="output_vpc_cidr_block"></a> [vpc\_cidr\_block](#output\_vpc\_cidr\_block) | CIDR block of the VPC |
| <a name="output_zookeeper_private_ips"></a> [zookeeper\_private\_ips](#output\_zookeeper\_private\_ips) | Zookeper instances private IP |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
