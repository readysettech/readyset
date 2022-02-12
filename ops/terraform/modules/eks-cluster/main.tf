
#-------------- [ Supplemental Tags Required for EKS ] ---------------- #

module "vpc-subnet-tags" {
  source                 = "./modules/eks-vpc-tags"
  cluster_name           = var.cluster_name
  vpc_id                 = var.vpc_id
  create_subnet_elb_tags = var.create_subnet_elb_tags
}

#-------------- [ EKS Cluster ] --------------------------------------- #

# The core EKS module
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 18.5.1"

  # General
  cluster_name      = var.cluster_name
  cluster_version   = var.cluster_version
  cluster_ip_family = var.cluster_ip_family

  # Authentication
  enable_irsa = true

  # Networking
  subnet_ids = var.worker_subnet_ids
  vpc_id     = var.vpc_id
  cluster_security_group_additional_rules = {
    # Note: Key names in this map are arbitrary.
    # Name as you will.
    egress_nodes_ephemeral_ports_tcp = {
      description                = "To node 1025-65535"
      protocol                   = "tcp"
      from_port                  = 1025
      to_port                    = 65535
      type                       = "egress"
      source_node_security_group = true
    }
    ingress_vpn_https = {
      description = "Ingress HTTPs from VPN"
      protocol    = "tcp"
      cidr_blocks = var.cluster_vpn_cidr_blocks
      from_port   = 443
      to_port     = 443
      type        = "ingress"
    }
  }

  # Per the following docs, this is the way to allow inter-node workload traffic
  # https://docs.aws.amazon.com/eks/latest/userguide/sec-group-reqs.html
  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all ports/protocols"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
    egress_all = {
      description      = "Node all egress"
      protocol         = "-1"
      from_port        = 0
      to_port          = 0
      type             = "egress"
      cidr_blocks      = ["0.0.0.0/0"]
      ipv6_cidr_blocks = ["::/0"]
    }
    ingress_allow_access_from_control_plane = {
      type                          = "ingress"
      protocol                      = "tcp"
      from_port                     = 9443
      to_port                       = 9443
      source_cluster_security_group = true
      description                   = "Allow access from control plane to webhook port of AWS load balancer controller"
    }
  }

  # Security
  cluster_endpoint_private_access = var.cluster_endpoint_private_only_access
  cluster_endpoint_public_access  = var.cluster_endpoint_private_only_access ? false : true
  cluster_encryption_config = [{
    provider_key_arn = var.kms_key_arn
    resources        = ["secrets"]
  }]
  cluster_addons = {
    coredns = {
      resolve_conflicts = "OVERWRITE"
    }
    kube-proxy = {}
    vpc-cni = {
      resolve_conflicts = "OVERWRITE"
    }
  }

  # Logs
  cluster_enabled_log_types              = var.cluster_log_types
  cloudwatch_log_group_retention_in_days = var.cluster_log_retention_days

  # Self Managed Node Group(s)
  self_managed_node_groups = var.self_managed_node_groups
  self_managed_node_group_defaults = merge(
    var.self_managed_node_group_defaults, {
      vpc_security_group_ids       = [aws_security_group.supplemental.id]
      iam_role_additional_policies = ["arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"]
  })

  tags = var.resource_tags
}

# Custom/Supplemental VPC SG separate from complex k8s sg
resource "aws_security_group" "supplemental" {
  name   = format("%s-supplemental", var.cluster_name)
  vpc_id = var.vpc_id
  dynamic "ingress" {
    for_each = length(var.workers_ssh_cidr_allowed) > 0 ? ["create"] : []
    content {
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = var.workers_ssh_cidr_allowed
    }
  }
}

# Kubernetes Namespaces
resource "kubernetes_namespace" "this" {
  for_each = toset(var.kubernetes_namespaces)

  metadata {
    name = each.value
  }
  depends_on = [module.eks]
}

################################################################################
# aws-auth configmap
# Only EKS managed node groups automatically add roles to aws-auth configmap
# so we need to ensure fargate profiles and self-managed node roles are added
################################################################################

resource "null_resource" "apply" {
  triggers = {
    kubeconfig = base64encode(local.kubeconfig)
    cmd_patch  = <<-EOT
      kubectl create configmap aws-auth -n kube-system --kubeconfig <(echo $KUBECONFIG | base64 --decode)
      kubectl patch configmap/aws-auth --patch "${module.eks.aws_auth_configmap_yaml}" -n kube-system --kubeconfig <(echo $KUBECONFIG | base64 --decode)
    EOT
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      KUBECONFIG = self.triggers.kubeconfig
    }
    command = self.triggers.cmd_patch
  }
}

# -------------- [ Cluster Autoscaler ] -------------------------------- #
# This facilitates autoscaling of the underlying EC2 infrastructure
module "cluster-autoscaler" {
  source = "./modules/cluster-autoscaler"
  count  = var.cluster_autoscaler_enabled ? 1 : 0

  aws_region              = var.aws_region
  cluster_id              = module.eks.cluster_id
  cluster_oidc_issuer_url = module.eks.cluster_oidc_issuer_url
  resource_tags           = var.resource_tags
}

# -------------- [ Node Termination Handler ] -------------------------- #
# Node Termination Handler ensures that pods running on EKS worker nodes
# are drained and migrated to new hosts before terminating the node.
module "node-term-handler" {
  count  = var.node_termination_handler_enabled ? 1 : 0
  source = "./modules/node-termination-handler"

  # Environment Configs
  aws_region         = var.aws_region
  cluster_name       = var.cluster_name
  helm_chart_version = lookup(local.chart_versions_merged, "node_term_handler")

  # Identity Provider
  cluster_oidc_issuer_url = module.eks.cluster_oidc_issuer_url

  # Workers
  self_managed_node_groups = module.eks.self_managed_node_groups

  # General
  resource_tags = var.resource_tags
}

# -------------- [ External DNS ] --------------------------------------- #
# External-DNS that dynamically creates private hosted zone records
# that typically map to an internal-only load balancer
module "externaldns-internal" {
  count  = var.external_dns_internal_enabled ? 1 : 0
  source = "./modules/external-dns"
  providers = {
    aws     = aws
    aws.dns = aws.dns
  }

  # Environment Configs
  cluster_id         = module.eks.cluster_id
  helm_chart_version = lookup(local.chart_versions_merged, "external_dns")

  # Route53 Configs
  r53_domain    = var.external_dns_private_zone_domain
  dns_zone_mode = var.external_dns_internal_zone_mode

  # Identity Provider
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer_url   = module.eks.cluster_oidc_issuer_url
}

# -------------- [ AWS-LB-Controller ] ----------------------------------- #

module "aws-lb-controller" {
  count  = var.aws_lb_controller_enabled ? 1 : 0
  source = "./modules/aws-lb-controller"

  # Environment Configs
  aws_region         = var.aws_region
  cluster_name       = var.cluster_name
  helm_chart_version = lookup(local.chart_versions_merged, "aws_lb_controller")

  # Identity Provider
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer_url   = module.eks.cluster_oidc_issuer_url

  # General
  resource_tags = var.resource_tags
}

# -------------- [ K8S Ingresses ] -------------------------- #
# For each namespace, create an ingress resource
module "k8s-ingress-internal" {
  for_each = var.alb_internal_ingress_enabled ? var.ns_ingress_routing_rules : toset({})
  source   = "./modules/k8s-ingress"

  # Environment Configs
  environment  = var.environment
  aws_region   = var.aws_region
  cluster_name = module.eks.cluster_id

  # Ingress Chart
  helm_deployment_namespace = each.key
  ns_ingress_routing_rules  = each.value
  acm_cert_arn              = var.alb_acm_cert_arn

  depends_on = [kubernetes_namespace.this]
}

# -------------- [ Benchmark Prom & Grafana ] -------------------------------- #

module "benchmark-prom-grafana" {
  source             = "./modules/prom-grafana"
  count              = var.benchmark_prom_grafana_enabled ? 1 : 0
  helm_chart_version = lookup(local.chart_versions_merged, "bench_prom_grafana")
  grafana_password   = data.aws_ssm_parameter.grafana_pass.value
}

# -------------- [ Image Pull Secret Refresher ] ----------------------------- #

module "ips-refresher" {
  source = "./modules/ips-refresher"
  count  = var.ips_refresher_enabled ? 1 : 0

  # ECR Repo Configs
  authorized_ecr_resource_arns = var.ips_refresher_authorized_ecr_resource_arns
  ecr_account_id               = var.ips_refresher_ecr_aws_account_id

  # Environment Configs
  aws_region   = var.aws_region
  cluster_name = var.cluster_name

  # General
  resource_tags = var.resource_tags

  # Identity Provider
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer_url   = module.eks.cluster_oidc_issuer_url
}
