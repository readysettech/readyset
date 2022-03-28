locals {
  kubeconfig = yamlencode({
    apiVersion      = "v1"
    kind            = "Config"
    current-context = "terraform"
    clusters = [{
      name = module.eks.cluster_id
      cluster = {
        certificate-authority-data = module.eks.cluster_certificate_authority_data
        server                     = module.eks.cluster_endpoint
      }
    }]
    contexts = [{
      name = "terraform"
      context = {
        cluster = module.eks.cluster_id
        user    = "terraform"
      }
    }]
    users = [{
      name = "terraform"
      user = {
        token = data.aws_eks_cluster_auth.this.token
      }
    }]
  })
  # Self-Managed Nodes AWS Auth
  self_managed_ec2_auth = [for group in module.eks.self_managed_node_groups : {
    rolearn = group.iam_role_arn,
    groups = ["system:bootstrappers", "system:nodes" ],
    username = "system:node:{{EC2PrivateDNSName}}"
  }]
  # AWS-Auth CM Configuration Logistics
  merged_map_roles = replace(yamlencode(concat(
    local.self_managed_ec2_auth,
    var.map_roles,
  )), "\"", "")
  merged_map_users = replace(yamlencode(distinct(concat(
    try(yamldecode(yamldecode(module.eks.aws_auth_configmap_yaml).data.mapUsers), []),
    var.map_users,
  ))), "\"", "")
  merged_map_accounts = replace(yamlencode(distinct(concat(
    try(yamldecode(yamldecode(module.eks.aws_auth_configmap_yaml).data.mapAccounts), []),
    var.map_accounts,
  ))), "\"", "")
  aws_auth_configmap_yaml = templatefile("${path.module}/templates/aws-auth-cm.yaml.tpl", {
    mapAccounts = local.merged_map_accounts
    mapRoles    = local.merged_map_roles
    mapUsers    = local.merged_map_users
  })

  # Merged version matrix for Helm charts, considering defaults
  # and any overrides requested in var.chart_version
  chart_versions_merged      = merge(var.chart_version_defaults, var.chart_versions)
  benchmark_grafana_ssm_path = format("/%s%s", module.eks.cluster_id, var.benchmark_ssm_path_grafana_password)
}
