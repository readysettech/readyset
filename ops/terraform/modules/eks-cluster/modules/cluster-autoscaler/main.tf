
module "iam_assumable_role_cluster_autoscaler" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "~> 4.0"

  create_role      = true
  role_name_prefix = "cluster-autoscaler"
  role_description = "IRSA role for cluster autoscaler"

  provider_url                   = replace(var.cluster_oidc_issuer_url, "https://", "")
  role_policy_arns               = [aws_iam_policy.cluster_autoscaler.arn]
  oidc_fully_qualified_subjects  = [format("system:serviceaccount:%s:%s", var.helm_deployment_namespace, "cluster-autoscaler-aws")]
  oidc_fully_qualified_audiences = ["sts.amazonaws.com"]

  tags = var.resource_tags
}

resource "aws_iam_policy" "cluster_autoscaler" {
  name   = "KarpenterControllerPolicy-refresh"
  policy = data.aws_iam_policy_document.cluster_autoscaler.json
  tags   = var.resource_tags
}
