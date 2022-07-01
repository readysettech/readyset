locals {
  secrets_manager_kms_key = length(var.iam_authorized_secrets_manager_kms_key_arn) == 0 ? data.aws_kms_key.default-sm.arn : var.iam_authorized_secrets_manager_kms_key_arn
  auth0_frontend_iam_role_name = format("auth0-frontend-role-%s-%s-%s",
    var.quality,
    var.environment,
    var.aws_region
  )
  auth0_frontend_iam_policy_name = format("auth0-frontend-iam-%s-%s-%s",
    var.quality,
    var.environment,
    var.aws_region
  )
}