## AWS-Load-Balancer-Controller Terraform Module

The purpose of this module is to deploy the LB-Ingress-Controller (aka aws-load-balancer-controller) Helm chart into the destination Kubernetes cluster. This chart is powered by IRSA (IAM Roles for Service Accounts) which provides a means of federating access to IAM roles to k8s service accounts. To that end, there is an IAM role and relevant policies encapsulated in this module for easy deployment into an AWS/EKS environment.

For more information on what the LB-Ingress-Controller does, check out the [documentation here](https://github.com/kubernetes-sigs/aws-load-balancer-controller).

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | n/a |
| <a name="provider_helm"></a> [helm](#provider\_helm) | n/a |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [aws_iam_role.aws_lb_controller](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role_policy.aws-lb-controller-policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [helm_release.aws_lb_controller](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |
| [aws_iam_policy_document.aws_lb_controller](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_cluster_name"></a> [cluster\_name](#input\_cluster\_name) | The name of the k8s cluster being deployed into. | `string` | n/a | yes |
| <a name="input_helm_chart_name"></a> [helm\_chart\_name](#input\_helm\_chart\_name) | Name of the chart in helm\_chart\_repository that's to be installed. | `string` | `"aws-load-balancer-controller"` | no |
| <a name="input_helm_chart_repository"></a> [helm\_chart\_repository](#input\_helm\_chart\_repository) | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://aws.github.io/eks-charts"` | no |
| <a name="input_helm_chart_version"></a> [helm\_chart\_version](#input\_helm\_chart\_version) | Version of the Helm Chart to deploy. | `string` | `"1.3.3"` | no |
| <a name="input_helm_deployment_name"></a> [helm\_deployment\_name](#input\_helm\_deployment\_name) | Name of the Helm release to be created. | `string` | `"aws-load-balancer-controller"` | no |
| <a name="input_helm_deployment_namespace"></a> [helm\_deployment\_namespace](#input\_helm\_deployment\_namespace) | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| <a name="input_helm_service_account_name"></a> [helm\_service\_account\_name](#input\_helm\_service\_account\_name) | Name of the k8s service account to bind the created IAM role to. | `string` | `"aws-load-balancer-controller"` | no |
| <a name="input_oidc_issuer_url"></a> [oidc\_issuer\_url](#input\_oidc\_issuer\_url) | OIDC issuer URL for the EKS cluster. | `any` | n/a | yes |
| <a name="input_oidc_provider_arn"></a> [oidc\_provider\_arn](#input\_oidc\_provider\_arn) | The OIDC provider ARN for the EKS cluster. | `string` | n/a | yes |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |

## Outputs

No outputs.
