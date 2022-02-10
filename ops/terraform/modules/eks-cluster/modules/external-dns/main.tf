#-------------- [ IAM ] -------------------------------------------------- #

# Role that'll be assumed in k8s by the service account
resource "aws_iam_role" "externaldns" {
  name               = format("%s-%s", local.name, var.cluster_id)
  assume_role_policy = data.aws_iam_policy_document.externaldns.json
}

resource "aws_iam_role_policy" "externaldns" {
  name   = format("%s-%s", local.name, var.cluster_id)
  role   = aws_iam_role.externaldns.id
  policy = data.aws_iam_policy_document.externaldns-policy.json
}

#-------------- [ IAM in Zone Account ] -------------------------------------------------- #

# Cross-Account role for ExternalDNS to assume
module "ext-dns-cross-account-zone-role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role"
  version = "~> 4"
  providers = {
    aws = aws.dns
  }
  role_name         = format("%s-%s-xaccount-zones", local.name, var.cluster_id)
  create_role       = var.external_dns_cross_account_zone
  role_requires_mfa = false
  custom_role_policy_arns = [
    # Role in Zone account
    aws_iam_policy.externaldns-xaccount[0].arn
  ]
  # List of roles that are trusted to assume this one.
  trusted_role_arns = [
    aws_iam_role.externaldns.arn,
    format("arn:aws:iam::%s:root", data.aws_caller_identity.current-dns[0].account_id),
  ]
}

resource "aws_iam_policy" "externaldns-xaccount" {
  count    = var.external_dns_cross_account_zone ? 1 : 0
  provider = aws.dns
  name     = format("%s-%s-xaccount", local.name, var.cluster_id)
  description = format(
    "Federates DNS zone record creation capabilities to account: %s",
    data.aws_caller_identity.current.account_id
  )
  policy = data.aws_iam_policy_document.xaccount-zone[count.index].json
}

#-------------- [ Helm ] ------------------------------------------------- #

resource "helm_release" "externaldns" {
  name       = local.name
  repository = var.helm_chart_repository
  chart      = var.helm_chart_name
  namespace  = var.helm_deployment_namespace
  version    = var.helm_chart_version
  values = [
    templatefile("${path.module}/templates/helm/values.yaml", {
      region        = data.aws_region.current.name
      id            = data.aws_route53_zone.zone.zone_id
      account       = data.aws_caller_identity.current.account_id
      sa            = aws_iam_role.externaldns.arn
      zoneType      = var.dns_zone_mode
      name          = local.name
      assumeRoleArn = local.dns_role_assume
      r53Domain     = var.r53_domain
    })
  ]
}
