
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
  source = "terraform-aws-modules/eks/aws"

  # General
  cluster_name      = var.cluster_name
  cluster_version   = var.cluster_version
  cluster_ip_family = var.cluster_ip_family

  # Authentication
  enable_irsa = true

  # Networking
  subnet_ids = var.worker_subnet_ids
  vpc_id     = var.vpc_id

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
  self_managed_node_group_defaults = {
    vpc_security_group_ids       = [aws_security_group.supplemental.id]
    iam_role_additional_policies = ["arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"]
  }

  tags = merge(var.resource_tags, {
    Name = var.cluster_name
  })
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
  depends_on = [
    null_resource.apply,
  ]
}

# -------------- [ Node Termination Handler ] -------------------------- #
# Node Termination Handler ensures that pods running on EKS worker nodes
# are drained and migrated to new hosts before terminating the node.
module "node-term-handler" {
  count              = var.node_termination_handler_enabled ? 1 : 0
  source             = "./modules/node-termination-handler"
  helm_chart_version = lookup(local.chart_versions_merged, "node_term_handler")

  aws_region               = var.aws_region
  cluster_name             = var.cluster_name
  cluster_oidc_issuer_url  = module.eks.cluster_oidc_issuer_url
  self_managed_node_groups = module.eks.self_managed_node_groups
  resource_tags            = var.resource_tags
  depends_on = [
    null_resource.apply,
  ]
}

# External-DNS that dynamically creates private hosted zone records
# that typically map to an internal-only load balancer
module "externaldns-internal" {
  count              = var.external_dns_internal_enabled ? 1 : 0
  source             = "./modules/external-dns"
  cluster_id         = module.eks.cluster_id
  helm_chart_version = lookup(local.chart_versions_merged, "external_dns")

  # Route53 Configs
  r53_domain    = var.external_dns_private_zone_domain
  dns_zone_mode = "private"

  # Identity Provider
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_issuer_url   = module.eks.cluster_oidc_issuer_url
}
