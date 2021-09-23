locals {
  account_id  = data.aws_caller_identity.current.account_id
  region_name = data.aws_region.current.name

  artifacts_bucket_name        = "readysettech-${var.environment}-buildkite-artifacts-${local.region_name}"
  artifacts_bucket_arn         = "arn:aws:s3:::${local.artifacts_bucket_name}"
  artifacts_bucket_objects_arn = "${local.artifacts_bucket_arn}/*"

  secrets_bucket_name        = "readysettech-${var.environment}-buildkite-secrets-${local.region_name}"
  secrets_bucket_arn         = "arn:aws:s3:::${local.secrets_bucket_name}"
  secrets_bucket_objects_arn = "${local.secrets_bucket_arn}/*"

  buildkite_agent_token_secret_name          = "buildkite/AGENT_TOKEN"
  buildkite_agent_token_parameter_store_path = "/aws/reference/secretsmanager/${local.buildkite_agent_token_secret_name}"
}

# TODO: Support multi region better by setting up replication
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

resource "aws_s3_bucket" "secrets" {
  bucket = local.secrets_bucket_name
  tags = {
    Name = local.secrets_bucket_name
  }
  versioning {
    enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "secrets" {
  bucket = aws_s3_bucket.secrets.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifacts_bucket_name
  tags = {
    Name = local.artifacts_bucket_name
  }
  versioning {
    enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket = aws_s3_bucket.artifacts.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# AWS Secret Manager policies must point at roles that already exist. Because of
# this, leave the policy out of the configuration for now until we take over
# more of the IAM roles for Buildkite.
resource "aws_secretsmanager_secret" "buildkite_agent_token" {
  # We only want to set up the secrets manager in us-east-2 and replicate to other regions
  count = local.region_name == "us-east-2" ? 1 : 0
  name  = local.buildkite_agent_token_secret_name
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        AWS = var.agent_token_allowed_roles
      },
      Action   = "secretsmanager:GetSecretValue",
      Resource = "*",
    }]
  })
}
