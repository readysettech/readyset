resource "helm_release" "this" {
  name      = var.helm_deployment_name
  chart     = "${path.module}/helm"
  namespace = var.helm_deployment_namespace
  values = [
    data.utils_deep_merge_yaml.values-file.output
  ]
  force_update = false
}
