## EKS-Vpc-Tags Terraform Module

The purpose of this Terraform module is to provide an easy way to add the required VPC subnet tags to a VPC's subnets so that a Kubernetes cluster and its' supporting/foundational components (like the `alb-ingress-controller`) can auto-detect the public and private subnets which underpin the cluster.

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| aws | n/a |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| cluster\_name | Name of the EKS cluster to provision tags for. | `string` | n/a | yes |
| create\_subnet\_elb\_tags | Toggles creation of necessary subnet tags which underpin auto-subnet discovery of LB Ingress Controller. | `bool` | `true` | no |
| vpc\_id | ID of the VPC to deploy resources within. | `string` | n/a | yes |
| vpc\_private\_subnet\_data\_value | Data source values to query VPC in order to locate private subnets. | `list(string)` | <pre>[<br>  "private"<br>]</pre> | no |
| vpc\_public\_subnet\_data\_value | Data source values to query VPC in order to locate public subnets. | `list(string)` | <pre>[<br>  "public"<br>]</pre> | no |
| vpc\_subnet\_data\_source\_key | Data source key to query VPC in order to locate various subnets. | `string` | `"tag:Connectivity"` | no |

## Outputs

No output.
