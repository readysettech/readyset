# Module that deploys EKS cluster, Helm charts, etc.
module "eks-main" {
  source = "../../modules/eks-cluster"
  providers = {
    aws     = aws
    aws.dns = aws.dns
  }

  # General
  aws_region    = var.aws_region
  resource_tags = var.resource_tags
  environment   = var.environment

  # Namespaces that will be created in k8s by the module
  kubernetes_namespaces = var.kubernetes_namespaces

  # Cluster Configurations
  cluster_name                         = var.cluster_name
  cluster_version                      = var.cluster_version
  cluster_endpoint_private_only_access = var.cluster_endpoint_private_only_access
  cluster_ip_family                    = var.cluster_ip_family
  cluster_service_ipv4_cidr            = var.cluster_service_ipv4_cidr
  cluster_vpn_cidr_blocks              = var.cluster_vpn_cidr_blocks

  # Workers
  //self_managed_node_groups         = [ for k, v in var.self_managed_node_group_configs : {k = v} if lookup(v, "single_az", "false") !=  "true" ]
  self_managed_node_groups = {
    for index, x in var.self_managed_node_group_configs :
    index => merge(x, {
      subnet_ids = (
        lookup(x, "single_az", "") == "true" ? [local.private_subnet_ids_list[0]] : local.private_subnet_ids_list
      )
      }
    )
  }

  self_managed_node_group_defaults = var.self_managed_node_group_defaults

  # Networking / Security
  kms_key_arn              = aws_kms_key.eks-main.arn
  vpc_id                   = var.vpc_id
  vpc_dns_resolver_ip      = var.vpc_dns_resolver_ip
  workers_ssh_cidr_allowed = var.workers_ssh_cidr_allowed
  worker_subnet_ids        = data.aws_subnet_ids.private.ids

  # Logging
  cluster_log_retention_days = var.cluster_log_retention_days
  cluster_log_types          = var.cluster_log_types

  # Addon Helm Charts
  chart_versions                   = var.chart_versions
  cluster_autoscaler_enabled       = var.cluster_autoscaler_enabled
  node_termination_handler_enabled = var.node_termination_handler_enabled
  # ExternalDNS
  external_dns_internal_enabled    = var.external_dns_internal_enabled
  external_dns_external_enabled    = var.external_dns_external_enabled
  external_dns_pub_zone_domain     = var.external_dns_pub_zone_domain
  external_dns_private_zone_domain = var.external_dns_private_zone_domain
  external_dns_internal_zone_mode  = var.external_dns_internal_zone_mode

  # Ingresses
  alb_internal_ingress_enabled = true
  alb_acm_cert_arn             = var.alb_acm_cert_arn
  ns_ingress_routing_rules     = var.ns_ingress_routing_rules

  # Prometheus / Grafana
  benchmark_prom_grafana_enabled = var.benchmark_prom_grafana_enabled


}

# KMS key for Kubernetes Secret Wrapper Encryption
resource "aws_kms_key" "eks-main" {
  deletion_window_in_days = 30
  enable_key_rotation     = true
  description             = format("Used for EKS cluster encryption in %s.", var.cluster_name)
  tags                    = merge(var.resource_tags, { Name = local.eks_main_kms_key_name })
}
