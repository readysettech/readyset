resource "random_string" "auth0_secret" {
  length  = 64
  special = false
}

resource "random_string" "session_secret" {
  length  = 64
  special = false
}

# Default secrets manager key for when a KMS key arn isn't supplied.
data "aws_kms_key" "default-sm" {
  key_id = "alias/aws/secretsmanager"
}

data "aws_iam_policy_document" "auth0-frontend" {
  statement {
    sid    = "getSecret"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "ssm:GetParameter",
    ]
    resources = [var.auth0_rs_app_client_secret_arn]
  }

  statement {
    sid    = "kmsDecrypt"
    effect = "Allow"

    actions = [
      "kms:Decrypt*",
      "kms:Generate*",
    ]

    # ARN of KMS key that was used to encrypt Secrets Manager secrets
    resources = [local.secrets_manager_kms_key]
  }
}

data "template_file" "launch-script" {
  template = file("${path.module}/templates/auth0-frontend-userdata.tpl")
  vars = {
    auth0_client_id                = var.auth0_client_id
    auth0_domain                   = var.auth0_domain
    auth0_audience                 = var.auth0_audience
    issuer_base_url                = var.issuer_base_url
    redirect_uri                   = var.redirect_uri
    logout_uri                     = var.logout_uri
    auth0_rs_app_client_secret_arn = var.auth0_rs_app_client_secret_arn
    session_secret                 = random_string.session_secret.result
    auth0_secret                   = random_string.auth0_secret.result
  }
}