
locals {
  # Base set of interpolated values for the chart
  base_ingress_config = templatefile("${path.module}/helm/values/cluster-values.yaml", {
    name         = var.cluster_name
    region       = var.aws_region
    environment  = var.environment
    namespace    = var.helm_deployment_namespace
    acm_cert_arn = var.acm_cert_arn
    lb_class     = var.ingress_lb_class
  })
  # Where the host rules are generated. This allows host routing rules
  # to be determined by the calling module vs in this module
  ng_ingress_routing_rules = replace(yamlencode({
    ingress : {
      hosts : var.ns_ingress_routing_rules
    }
  }), "\"", "")
}
