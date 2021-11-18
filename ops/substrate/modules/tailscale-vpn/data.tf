data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "aws_subnet_ids" "private" {
  vpc_id = var.vpc_id

  tags = {
    Connectivity = "private"
  }
}

data "aws_subnet_ids" "public" {
  vpc_id = var.vpc_id

  tags = {
    Connectivity = "public"
  }
}

data "aws_iam_policy_document" "subnet-router" {
  statement {
    sid    = "getSecret"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue"
    ]

    resources = [var.iam_authorized_secrets_manager_arn]
  }

  dynamic "statement" {
    for_each = length(var.iam_authorized_secrets_manager_kms_key_arn) > 0 ? ["enabled"] : []
    content {
      sid    = "kmsDecrypt"
      effect = "Allow"

      actions = [
        "kms:Decrypt*",
        "kms:Generate*",
      ]

      # ARN of KMS key that was used to encrypt Secrets Manager secrets
      resources = [var.iam_authorized_secrets_manager_kms_key_arn]
    }
  }
}
