# Node-Termination-Handler Terraform Module for EKS

The purpose of this Terraform module is to encapsulate the complexity that is automated Kubelet node draining for AWS EKS. Instead of EC2 worker nodes getting terminated and requests failing, one can implement the node-termination-handler in their cluster and AWS environment to shift workloads to another node before the instance is culled. Ultimately, this leads to fewer service interruptions and empowers ephemeral clusters.

For more information on this implementation, check out the [docs here](https://github.com/aws/aws-node-termination-handler).

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
| cluster\_name | The name of the k8s cluster being deployed into. | `string` | n/a | yes |
| cluster\_oidc\_issuer\_url | OIDC issuer URL for the EKS cluster. | `string` | n/a | yes |
| helm\_chart\_name | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"aws-node-termination-handler"` | no |
| helm\_chart\_repository | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://aws.github.io/eks-charts"` | no |
| helm\_chart\_version | Version of Helm Chart to deploy. | `string` | `"0.16.0"` | no |
| helm\_deployment\_name | Name of the Helm release to create. | `string` | `"aws-node-termination-handler"` | no |
| helm\_deployment\_namespace | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| resource\_tags | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |
| self\_managed\_node\_groups | Self-managed node group configs outputted by EKS module. | `any` | n/a | yes |

## Outputs

No output.
