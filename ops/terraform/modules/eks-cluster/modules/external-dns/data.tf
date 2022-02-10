#-------------- [ AWS ] ------------------------------------------------- #

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

#-------------- [ EKS ] ------------------------------------------------- #

data "aws_eks_cluster_auth" "eks" {
  name = var.cluster_id
}

#-------------- [ R53 ] ------------------------------------------------- #
# Locates the route53 zone we'll be creating records in
data "aws_route53_zone" "externaldns" {
  name         = var.r53_domain
  private_zone = var.dns_zone_mode == "public" ? false : true
}

# Authorize externalDns to manage DNS records
data "aws_iam_policy_document" "externaldns-policy" {
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
      "arn:aws:route53:::hostedzone/${data.aws_route53_zone.externaldns.zone_id}"
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
