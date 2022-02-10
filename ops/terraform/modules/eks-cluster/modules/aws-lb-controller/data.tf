data "aws_iam_policy_document" "aws_lb_controller" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    effect  = "Allow"

    condition {
      test     = "StringEquals"
      variable = format("%s:sub", replace(var.oidc_issuer_url, "https://", ""))
      values = [
        format(
          "system:serviceaccount:%s:%s",
          var.helm_deployment_namespace,
          var.helm_service_account_name
        )
      ]
    }

    principals {
      identifiers = [var.oidc_provider_arn]
      type        = "Federated"
    }
  }
}
