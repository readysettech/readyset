## Cluster-Autoscaler Terraform Module for EKS

The purpose of this module is to provide an easy way to deploy the Cluster-Autoscaler Helm chart into a given Kubernetes cluster and AWS account. AWS IAM components are also encapsulated in this module to isolate permissions between clusters.

For more information about Cluster-Autoscaler, check out the [docs here](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler).

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
| aws\_region | The AWS region to create resources in. | `string` | n/a | yes |
| cluster\_id | ID of the EKS cluster to be augmented with cluster autoscaling. | `any` | n/a | yes |
| cluster\_oidc\_issuer\_url | OIDC issuer URL for the EKS cluster. | `any` | n/a | yes |
| helm\_chart\_name | Name of the Cluster Autoscaler chart in helm\_chart\_repository. | `string` | `"cluster-autoscaler"` | no |
| helm\_chart\_repository | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://kubernetes.github.io/autoscaler"` | no |
| helm\_chart\_version | Version of Cluster Autoscaler Helm Chart to deploy. | `string` | `"9.12.0"` | no |
| helm\_deployment\_name | Name of the Cluster Autoscaler Helm release to create. | `string` | `"cluster-autoscaler"` | no |
| helm\_deployment\_namespace | Namespace to deploy the Cluster Autoscaler Helm chart into. | `string` | `"kube-system"` | no |
| resource\_tags | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |

## Outputs

No output.
