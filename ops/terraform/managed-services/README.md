# Managed-Services

A place in the internal Terraform hierarchy whereby managed-service resources are deployed, such as IAM roles, Kubernetes resources, RDS instances, and the like.

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | = 1.0.2 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 3.74.0 |
| <a name="requirement_external"></a> [external](#requirement\_external) | >= 2.2.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | 4.3.0 |
| <a name="provider_aws.network"></a> [aws.network](#provider\_aws.network) | 4.3.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_ci-k8s-rbac"></a> [ci-k8s-rbac](#module\_ci-k8s-rbac) | ../modules/ci-runner-iam-k8s | n/a |

## Resources

| Name | Type |
|------|------|
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_eks_cluster.eks-primary](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/eks_cluster) | data source |
| [aws_eks_cluster_auth.eks-primary](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/eks_cluster_auth) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |
| [aws_subnets.private](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnets) | data source |
| [aws_subnets.public](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnets) | data source |
| [aws_vpc.network](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/vpc) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_eks_cluster_name"></a> [eks\_cluster\_name](#input\_eks\_cluster\_name) | Name of EKS cluster resources will be deployed within. | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment. | `string` | n/a | yes |
| <a name="input_kubernetes_namespaces"></a> [kubernetes\_namespaces](#input\_kubernetes\_namespaces) | List of Kubernetes namespaces for resources to be deployed within. | `list(string)` | `[]` | no |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |
| <a name="input_vpc_id"></a> [vpc\_id](#input\_vpc\_id) | ID of the VPC to deploy resources within. | `string` | n/a | yes |

## Outputs

No outputs.
