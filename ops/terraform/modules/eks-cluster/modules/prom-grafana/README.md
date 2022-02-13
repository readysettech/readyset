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
| [helm_release.prom-pushgw](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |

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
| <a name="input_prometheus_hostname"></a> [prometheus\_hostname](#input\_prometheus\_hostname) | Hostname that has been assigned to Prometheus service outside of this module. | `string` | `"prom-stack-kube-prometheus-prometheus"` | no |
| <a name="input_prometheus_metric_retention_days"></a> [prometheus\_metric\_retention\_days](#input\_prometheus\_metric\_retention\_days) | Retention period in days for Prometheus cluster metrics. | `string` | `"180d"` | no |
| <a name="input_prometheus_pod_resources"></a> [prometheus\_pod\_resources](#input\_prometheus\_pod\_resources) | Pod resource definitions for Prometheus pods deployed via the Operator. | `map` | <pre>{<br>  "requests": {<br>    "cpu": "500m",<br>    "memory": "1024Mi"<br>  }<br>}</pre> | no |
| <a name="input_pushgw_deployment_strategy"></a> [pushgw\_deployment\_strategy](#input\_pushgw\_deployment\_strategy) | Deployment strategy value to be provided to Push Gateway chart. | `string` | `"Recreate"` | no |
| <a name="input_pushgw_enabled"></a> [pushgw\_enabled](#input\_pushgw\_enabled) | Toggles creation of Prometheus Push Gateway resources.. | `bool` | `true` | no |
| <a name="input_pushgw_helm_chart_name"></a> [pushgw\_helm\_chart\_name](#input\_pushgw\_helm\_chart\_name) | Name of the Helm chart to install from helm\_chart\_repository. | `string` | `"prometheus-pushgateway"` | no |
| <a name="input_pushgw_helm_chart_repository"></a> [pushgw\_helm\_chart\_repository](#input\_pushgw\_helm\_chart\_repository) | Helm chart repository that hosts the helm\_chart\_name. | `string` | `"https://prometheus-community.github.io/helm-charts"` | no |
| <a name="input_pushgw_helm_chart_version"></a> [pushgw\_helm\_chart\_version](#input\_pushgw\_helm\_chart\_version) | Version of Helm Chart to deploy. | `string` | `"1.15.0"` | no |
| <a name="input_pushgw_helm_deployment_name"></a> [pushgw\_helm\_deployment\_name](#input\_pushgw\_helm\_deployment\_name) | Name of the Helm release to create. | `string` | `"prom-pushgw"` | no |
| <a name="input_pushgw_helm_deployment_namespace"></a> [pushgw\_helm\_deployment\_namespace](#input\_pushgw\_helm\_deployment\_namespace) | Namespace to deploy the Helm chart into. | `string` | `"build"` | no |
| <a name="input_pushgw_hostname"></a> [pushgw\_hostname](#input\_pushgw\_hostname) | Hostname that has been assigned to Prometheus Push Gateway, outside of this module. | `string` | `"prom-pushgw-prometheus-pushgateway"` | no |
| <a name="input_pushgw_replica_count"></a> [pushgw\_replica\_count](#input\_pushgw\_replica\_count) | Replica count for Push Gateway deployment in k8s. | `number` | `3` | no |
| <a name="input_pushgw_service_mon_namespace"></a> [pushgw\_service\_mon\_namespace](#input\_pushgw\_service\_mon\_namespace) | Service monitor's namespace. | `string` | `"build"` | no |

## Outputs

No outputs.
