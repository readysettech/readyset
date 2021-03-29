# Readyset Terraform module

Terraform module to deploy Readyset in AWS.

## Usage

```hcl
provider "aws" {
  region = "us-east-2"
}

module "readyset" {
  source = "git@github.com:readysettech/readyset-terraform-aws.git"

  vpc_id   = "vpc-06d7e602f802696f5"
  key_name = "username"
}
```

## How to connect to instances?

If you have the SSH key provided to the `key_name` Terraform variable you will be able to connect via SSH
to the instances using the `admin` user, if the network you are connecting from is allowed in a security group listed
in the variable `extra_security_groups`

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

| Name | Version |
|------|---------|
| terraform | >= 0.14.7 |
| aws | >= 3.33.0 |

## Providers

| Name | Version |
|------|---------|
| aws | >= 3.33.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| associate\_public\_ip\_addresses | Whether or not to associate a public IP address with all provisioned instances | `bool` | `true` | no |
| db\_name | Name of the MySQL database to replicate to Noria | `string` | `""` | no |
| db\_password | Password for the MySQL user to use to connect to RDS | `string` | `""` | no |
| db\_user | MySQL user to use to connect to RDS | `string` | `""` | no |
| debezium\_connector\_instance\_type | EC2 instance type to use for the Debezium Connector instance | `string` | `"t3.medium"` | no |
| debezium\_instance\_type | EC2 instance type to use for the Debezium instance | `string` | `"t3.medium"` | no |
| deployment | Unique identifier for the name of the Noria deployment | `string` | `"noria"` | no |
| enable\_rds\_connector | Whether to enable streaming writes from an Amazon RDS instance to Noria | `bool` | `false` | no |
| encrypt\_noria\_disk | Encrypt the EBS volume used to persist Noria base tables | `bool` | `false` | no |
| encrypt\_zookeeper\_disk | Encrypt the EBS volume used to persist Zookeeper state | `bool` | `false` | no |
| extra\_security\_groups | List of extra security groups to associate with all provisioned EC2 instances | `list(string)` | `[]` | no |
| kafka\_instance\_type | EC2 instance type to use for the Kafka instance | `string` | `"m5.xlarge"` | no |
| key\_name | Name of the EC2 key pair to use when provisioning instances | `string` | n/a | yes |
| mysql\_allowed\_cidr\_blocks | List of CIDR blocks that are allowed to connect to MySQL | `list(string)` | `[]` | no |
| noria\_disk\_kms\_key\_id | ARN for the KMS key ID to use to encrypt the noria volume. Ignored if encrypt\_noria\_disk = false. | `string` | `""` | no |
| noria\_disk\_size\_gb | Size of the disk, in gigabytes, to provision for persisting Noria base tables | `number` | `200` | no |
| noria\_memory\_bytes | Amount of memory, in bytes, to provide to Noria for partially materialized state (0 = unlimited) | `number` | `0` | no |
| noria\_mysql\_instance\_type | EC2 instance type to use for the Noria MySQL adapter instance | `string` | `"m5.2xlarge"` | no |
| noria\_quorum | Number of noria workers to wait for before starting | `number` | `1` | no |
| noria\_server\_instance\_type | EC2 instance type to use for the Noria server instance(s) | `string` | `"m5.2xlarge"` | no |
| noria\_shards | Number of shards to use in Noria (0 = disable sharding) | `number` | `0` | no |
| noria\_version | Version of Noria to deploy | `string` | `"2ec92b13"` | no |
| rds\_instance\_id | ID of the RDS instance to stream writes from. Required if enable\_rds\_connector is true | `string` | `""` | no |
| tables | List of tables to replicate from RDS | `list(string)` | `[]` | no |
| vpc\_id | ID of the VPC to deploy all resources into | `string` | n/a | yes |
| zookeeper\_disk\_kms\_key\_id | ARN for the KMS key ID to use to encrypt the zookeeper volume. Ignored if encrypt\_zookeeper\_disk = false. | `string` | `""` | no |
| zookeeper\_disk\_size\_gb | Size of the disk, in gigabytes, to provision for persisting Zookeeper state | `number` | `200` | no |
| zookeeper\_instance\_type | EC2 instance type to use for the Zookeeper instance(s) | `string` | `"m5.large"` | no |

## Outputs

| Name | Description |
|------|-------------|
| debezium\_connect\_security\_group\_id | ID of the security group created for the Debezium Connect instance. This security group must have access to port 3306 of the RDS DB instance, if specified. |
| debezium\_connector\_private\_ip | Debezium connector instance private IP. |
| debezium\_connector\_public\_ip | Debezium connector instance public IP. |
| debezium\_private\_ip | Debezium instance private IP. |
| debezium\_public\_ip | Debezium instance public IP. |
| kafka\_private\_ip | Kafka instance private IP. |
| kafka\_public\_ip | Kafka instance public IP. |
| noria\_mysql\_private\_ip | Noria MySQL instance private IP. |
| noria\_mysql\_public\_ip | Noria MySQL instance public IP. |
| noria\_server\_private\_ip | Noria server instance private IP. |
| noria\_server\_public\_ip | Noria server instance public IP. |
| zookeeper\_private\_ip | Zookeper instance private IP. |
| zookeeper\_public\_ip | Zookeper instance public IP. |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
