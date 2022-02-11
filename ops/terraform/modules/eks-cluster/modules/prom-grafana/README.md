# Prometheus/Grafana Terraform Module

The purpose of this module is to get a reasonably well working Grafana and Prometheus cluster deployed into a destination k8s cluster. Persistent volumes are used for Prometheus pods in order to ensure the data isn't lost while pods move throughout the cluster.

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_helm"></a> [helm](#provider\_helm) | n/a |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [helm_release.kube-prometheus-stack](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_grafana_password"></a> [grafana\_password](#input\_grafana\_password) | Password for Grafana. | `string` | n/a | yes |
| <a name="input_helm_chart_name"></a> [helm\_chart\_name](#input\_helm\_chart\_name) | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"kube-prometheus-stack"` | no |
| <a name="input_helm_chart_repository"></a> [helm\_chart\_repository](#input\_helm\_chart\_repository) | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://prometheus-community.github.io/helm-charts"` | no |
| <a name="input_helm_chart_version"></a> [helm\_chart\_version](#input\_helm\_chart\_version) | Version of Helm Chart to deploy. | `string` | `"32.2.0"` | no |
| <a name="input_helm_deployment_name"></a> [helm\_deployment\_name](#input\_helm\_deployment\_name) | Name of the Helm release to create. | `string` | `"prom-stack"` | no |
| <a name="input_helm_deployment_namespace"></a> [helm\_deployment\_namespace](#input\_helm\_deployment\_namespace) | Namespace to deploy the Helm chart into. | `string` | `"build"` | no |
| <a name="input_prometheus_ebs_volume_size_gb"></a> [prometheus\_ebs\_volume\_size\_gb](#input\_prometheus\_ebs\_volume\_size\_gb) | Volume size in GiB for PVs created for Prometheus. For eb2, disk space directly impacts I/O, which could degrade performance. | `number` | `500` | no |
| <a name="input_prometheus_ebs_volume_storage_class"></a> [prometheus\_ebs\_volume\_storage\_class](#input\_prometheus\_ebs\_volume\_storage\_class) | Kubernetes storage class of PVs created for Prometheus. This typically points at a storage class backed by Amazon EBS CSI. | `string` | `"gp2"` | no |
| <a name="input_prometheus_metric_retention_days"></a> [prometheus\_metric\_retention\_days](#input\_prometheus\_metric\_retention\_days) | Retention period in days for Prometheus cluster metrics. | `string` | `"180d"` | no |
| <a name="input_prometheus_pod_resources"></a> [prometheus\_pod\_resources](#input\_prometheus\_pod\_resources) | Pod resource definitions for Prometheus pods deployed via the Operator. | `map` | <pre>{<br>  "requests": {<br>    "cpu": "500m",<br>    "memory": "1024Mi"<br>  }<br>}</pre> | no |

## Outputs

No outputs.
