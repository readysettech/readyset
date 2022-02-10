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
| utils | >= 0.17.14 |

## Providers

| Name | Version |
|------|---------|
| helm | n/a |
| utils | >= 0.17.14 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| acm\_cert\_arn | ACM certificate arn to be applied to ingress' listeners. | `string` | n/a | yes |
| aws\_region | The AWS region to create resources in. | `string` | n/a | yes |
| cluster\_name | The cluster ID for the EKS cluster. | `string` | n/a | yes |
| environment | The name of the environment. | `string` | n/a | yes |
| helm\_chart\_name | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"k8s-ingress"` | no |
| helm\_deployment\_name | Name of the Helm release to create. | `string` | `"k8s-ingress"` | no |
| helm\_deployment\_namespace | Namespace to deploy the Helm chart into. | `string` | `"kube-system"` | no |
| ingress\_lb\_class | Class of load balancer to annotate ingress resource with. | `string` | `"alb"` | no |
| ns\_ingress\_routing\_rules | Map of maps representing ingress hosts to be provisioned for the namespace. | `any` | n/a | yes |

## Outputs

No output.
