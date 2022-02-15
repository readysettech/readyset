resource "helm_release" "this" {
  name      = var.helm_deployment_name
  chart     = "${path.module}/helm"
  namespace = var.helm_deployment_namespace
  values = [
    templatefile("${path.module}/helm/values.yaml", {
      dns_upstream = var.vpc_dns_resolver_ip
    })
  ]
  force_update = true
}
