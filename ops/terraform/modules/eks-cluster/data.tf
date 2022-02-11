# Exposes AWS account ID and other metadata
data "aws_caller_identity" "current" {}

# current region
data "aws_region" "current" {}

#-------------- [ Network ] ---------------------------------------- #

data "aws_subnet_ids" "public" {
  vpc_id = var.vpc_id
  filter {
    name   = "tag:Connectivity"
    values = ["public"]
  }
}

data "aws_subnet_ids" "private" {
  vpc_id = var.vpc_id
  filter {
    name   = "tag:Connectivity"
    values = ["private"]
  }
}

# ---------- [ EKS Cluster Metadata ] ------------------------------ #

data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_id
}

# ---------- [ LB Ingress Controller ] ----------------------------- #

data "aws_route53_zone" "public" {
  count = var.external_dns_external_enabled ? 1 : 0
  name  = var.external_dns_pub_zone_domain
}

data "aws_route53_zone" "private" {
  count = var.external_dns_external_enabled ? 1 : 0
  name  = var.external_dns_private_zone_domain
}

# ---------- [ Benchmarking Prom/Grafana ] ------------------------- #

data "aws_ssm_parameter" "grafana_pass" {
  name = local.benchmark_grafana_ssm_path
}
