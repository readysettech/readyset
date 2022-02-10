#-------------- [ AWS ] ------------------------------------------------- #

data "aws_caller_identity" "current" {}

data "aws_caller_identity" "current-dns" {
  count    = var.external_dns_cross_account_zone ? 1 : 0
  provider = aws.dns
}

data "aws_region" "current" {}

#-------------- [ EKS ] ------------------------------------------------- #

data "aws_eks_cluster_auth" "eks" {
  name = var.cluster_id
}

#-------------- [ R53 ] ------------------------------------------------- #
# Locates the route53 zone we'll be creating records in

data "aws_route53_zone" "zone" {
  provider     = aws.dns
  name         = var.r53_domain
  private_zone = var.dns_zone_mode == "public" ? false : true
}

# Authorize externalDns to manage DNS records
data "aws_iam_policy_document" "externaldns-policy" {
  provider = aws.dns
  statement {
    sid    = "list"
    effect = "Allow"

    actions = [
      "route53:List*",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "edit"
    effect = "Allow"

    actions = [
      "route53:ChangeResourceRecordSets",
    ]

    resources = [
      "arn:aws:route53:::hostedzone/${data.aws_route53_zone.zone.zone_id}"
    ]
  }

  // Allow IRSA role to assume role in Zone management AWS account
  dynamic "statement" {
    for_each = toset(var.external_dns_cross_account_zone ? ["yes"] : [])
    content {
      sid     = "allowRoleAssume"
      effect  = "Allow"
      actions = ["sts:AssumeRole"]
      resources = [
        module.ext-dns-cross-account-zone-role.iam_role_arn
      ]
    }
  }
}

// Policy attached to role in the Zone hosting account.
data "aws_iam_policy_document" "xaccount-zone" {
  count    = var.external_dns_cross_account_zone ? 1 : 0
  provider = aws.dns
  statement {
    sid    = "list"
    effect = "Allow"

    actions = [
      "route53:List*",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "edit"
    effect = "Allow"

    actions = [
      "route53:ChangeResourceRecordSets",
    ]

    resources = [
      "arn:aws:route53:::hostedzone/${data.aws_route53_zone.zone.zone_id}"
    ]
  }
}

#-------------- [ IAM ] ------------------------------------------------- #

# Enable SA <-> IAM role binding
data "aws_iam_policy_document" "externaldns" {

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"

    condition {
      test     = "StringEquals"
      variable = format("%s:sub", replace(var.oidc_issuer_url, "https://", ""))
      values = [
        format("system:serviceaccount:%s:%s", var.helm_deployment_namespace, local.name)
      ]
    }

    principals {
      identifiers = [var.oidc_provider_arn]
      type        = "Federated"
    }
  }
}
