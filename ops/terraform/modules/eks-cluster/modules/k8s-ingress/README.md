# K8s-Ingress Chart & Terraform Module

The goal of this module is to provide a way to deploy an ingress to a Kubernetes namespace. The Helm chart module included provides one with already-enabled ssl redirection (handled at ALB).

When onboarding new services or hostnames, it is likely that you will need to change the inputs of this module's `ns\_ingress\_routing\_rules` variable downstream.

Here's an example:

```
ns_ingress_routing_rules = {
  build = [{
    fqdn        = "<hostname of new service>",
    service     = "<k8s service's name>",
    servicePort = "80",
    },
    // In complete example:
    {
      fqdn        = "benchmark-prom-demo.readyset.name",
      service     = "echo-service",
      servicePort = "80",
  }]
}
```

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_utils"></a> [utils](#requirement\_utils) | >= 0.17.14 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_helm"></a> [helm](#provider\_helm) | n/a |
| <a name="provider_utils"></a> [utils](#provider\_utils) | >= 0.17.14 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [helm_release.this](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |
| [utils_deep_merge_yaml.values-file](https://registry.terraform.io/providers/cloudposse/utils/latest/docs/data-sources/deep_merge_yaml) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_acm_cert_arn"></a> [acm\_cert\_arn](#input\_acm\_cert\_arn) | ACM certificate arn to be applied to ingress' listeners. | `string` | n/a | yes |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_cluster_name"></a> [cluster\_name](#input\_cluster\_name) | The cluster ID for the EKS cluster. | `string` | n/a | yes |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment. | `string` | n/a | yes |
| <a name="input_helm_chart_name"></a> [helm\_chart\_name](#input\_helm\_chart\_name) | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"k8s-ingress"` | no |
| <a name="input_helm_deployment_name"></a> [helm\_deployment\_name](#input\_helm\_deployment\_name) | Name of the Helm release to create. | `string` | `"k8s-ingress"` | no |
| <a name="input_helm_deployment_namespace"></a> [helm\_deployment\_namespace](#input\_helm\_deployment\_namespace) | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| <a name="input_ingress_lb_class"></a> [ingress\_lb\_class](#input\_ingress\_lb\_class) | Class of load balancer to annotate ingress resource with. | `string` | `"alb"` | no |
| <a name="input_ns_ingress_routing_rules"></a> [ns\_ingress\_routing\_rules](#input\_ns\_ingress\_routing\_rules) | Map of maps representing ingress hosts to be provisioned for the namespace. | `any` | n/a | yes |

## Outputs

No outputs.
