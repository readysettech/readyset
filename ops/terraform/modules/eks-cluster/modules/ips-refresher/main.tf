resource "helm_release" "this" {
  name             = var.helm_deployment_name
  chart            = "${path.module}/helm"
  namespace        = var.helm_deployment_namespace
  create_namespace = false

  values = [
    data.utils_deep_merge_yaml.values-file.output
  ]

  depends_on = [
    module.irsa-ips-refresher
  ]
}

#-------------- [ IRSA Role ] -------------------------------------- #

module "irsa-ips-refresher" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "~> 4.0"

  create_role      = true
  role_name        = local.irsa_role_name
  role_description = local.irsa_role_desc

  # OIDC Configs
  provider_url                   = local.irsa_oidc_provider_url
  oidc_fully_qualified_subjects  = local.irsa_oidc_qualified_subs
  oidc_fully_qualified_audiences = ["sts.amazonaws.com"]

  # Policy Management
  role_policy_arns = [aws_iam_policy.this.arn]

  tags = var.resource_tags
}

resource "aws_iam_policy" "this" {
  name   = local.irsa_role_policy_name
  policy = data.aws_iam_policy_document.this.json
  tags   = var.resource_tags
}

data "aws_iam_policy_document" "this" {
  statement {
    effect = "Allow"
    sid    = "ECRGrants"
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:GetRepositoryPolicy",
      "ecr:DescribeRepositories",
      "ecr:ListImages",
      "ecr:DescribeImages",
      "ecr:BatchGetImage",
      "ecr:GetLifecyclePolicy",
      "ecr:GetLifecyclePolicyPreview",
      "ecr:ListTagsForResource",
    ]
    resources = var.authorized_ecr_resource_arns
  }
}
