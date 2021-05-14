# Readyset multi-region-cluster

Terraform sub-module which creates two clusters in two different regions, including a VPC for each and a VPC peering
connection between them.

# Requirements

To use this Terraform module you will need the following things:

- An AWS user with permissions to deploy EC2 instances in `us-east-2` and `us-west-2` regions.
- An EC2 key pair with the same name in the aforementioned regions.
- The ReadySet version hash tag to be deployed.

## How to use this module?

Just clone the repository and run the following commands:

- `cd modules/multi-region-cluster`

The following command will download all needed modules:

- `terraform init`

The following command will deploy each cluster with the corresponding VPC in the established region:

- `terraform apply -target=module.readyset_primary -target=module.readyset_secondary`

Finally, establish a VPC peering connection between the two VPC deployed in the previous step:

- `terraform apply -target=module.vpc_peering`


Unfortunately just running `terraform apply` will not work because the VPC peering module needs the VPC to be created to
work properly.

# Terraform module information

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

| Name | Version |
|------|---------|
| terraform | >= 0.14.7 |
| aws | >= 3.33.0 |
| random | >= 3.1.0 |

## Providers

| Name | Version |
|------|---------|
| random | >= 3.1.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| key\_name | Name of the EC2 key pair to use when provisioning instances. | `string` | n/a | yes |
| readyset\_version | Readyset version to deploy (This is a required field, please ask for the latest version). | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| primary\_readyset\_mysql\_public\_ip | Readyset MySQL instance public IP. (Primary) |
| primary\_readyset\_server\_public\_ip | Readyset server instance public IP. (Primary) |
| primary\_vpc\_id | The ID of the VPC created in the provided AWS Region. (Primary) |
| primary\_zookeeper\_public\_ip | Zookeper instance public IP. (Primary) |
| secondary\_readyset\_mysql\_public\_ip | Readyset MySQL instance public IP. (Secondary) |
| secondary\_readyset\_server\_public\_ip | Readyset server instance public IP. (Secondary) |
| secondary\_vpc\_id | The ID of the VPC created in the provided AWS Region. (Secondary) |
| secondary\_zookeeper\_public\_ip | Zookeper instance public IP. (Secondary) |

<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
