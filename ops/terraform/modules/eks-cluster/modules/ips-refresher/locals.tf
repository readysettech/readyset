locals {
  # Chart values to be deepmerged together
  default_values = file("${path.module}/helm/values.yaml")
  cluster_values = templatefile("${path.module}/helm/values/cluster-values.yaml", {
    ecr_account_id  = var.ecr_account_id
    ecr_region      = var.aws_region
    ips_secret_name = var.ips_projected_secret_name
  })
  irsa_values = replace(yamlencode({
    irsa = {
      roleArn = module.irsa-ips-refresher.iam_role_arn
    }
  }), "\"", "")
  # IRSA role
  irsa_role_name        = format("%s-%s", var.cluster_name, "ips-refresher")
  irsa_role_desc        = format("IRSA role for IPS-Refresher in %s.", var.cluster_name)
  irsa_role_policy_name = format("%s-%s", var.cluster_name, "ips-refresher-policy")
  irsa_oidc_qualified_subs = [
    format(
      "system:serviceaccount:%s:%s",
      var.helm_deployment_namespace,
      var.k8s_service_account_name
    )
  ]
  irsa_oidc_provider_url = replace(var.oidc_issuer_url, "https://", "")
}
