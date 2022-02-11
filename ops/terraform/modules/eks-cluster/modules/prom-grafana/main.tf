resource "helm_release" "kube-prometheus-stack" {
  name             = var.helm_deployment_name
  namespace        = var.helm_deployment_namespace
  repository       = var.helm_chart_repository
  chart            = var.helm_chart_name
  version          = var.helm_chart_version
  create_namespace = false

  values = [
    templatefile("${path.module}/templates/helm/values.yaml", {
      prom_disk_space_gb      = var.prometheus_ebs_volume_size_gb
      prom_disk_storage_class = var.prometheus_ebs_volume_storage_class
      password                = var.grafana_password
      request_cpu             = lookup(var.prometheus_pod_resources, "requests")["cpu"]
      request_mem             = lookup(var.prometheus_pod_resources, "requests")["memory"]
      metric_reten_days       = var.prometheus_metric_retention_days
    })
  ]
}
