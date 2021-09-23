locals {
  instance_types = [
    "t3a.small",
    "c5a.4xlarge",
  ]
}

module "buildkite_queue_shared" {
  source           = "../../../../../modules/buildkite-queue-shared/regional"
  environment      = "build"
  buildkite_queues = [for it in local.instance_types : replace(it, ".", "-")]

  agent_token_allowed_roles = concat(
    tolist(module.buildkite_ops_queue.iam_roles),
    flatten(values(module.buildkite_queue)[*].iam_roles),
  )
}

module "buildkite_queue" {
  for_each = toset(local.instance_types)

  source      = "../../../../../modules/buildkite-queue/regional"
  environment = "build"

  buildkite_queue = replace(each.key, ".", "-")

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  secrets_bucket   = module.buildkite_queue_shared.secrets_bucket
  artifacts_bucket = module.buildkite_queue_shared.artifacts_bucket

  instance_type = each.key
  max_size      = 3
}

resource "aws_s3_bucket" "ops-secrets" {
  bucket = "readysettech-build-buildkite-ops-secrets-us-east-2"
  tags = {
    Name = "readysettech-build-buildkite-ops-secrets-us-east-2"
  }

  versioning {
    enabled = true
  }
}

# TODO: create this as part of the buildkite_queue_shared module
resource "aws_s3_bucket_public_access_block" "ops-secrets" {
  bucket = aws_s3_bucket.ops-secrets.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "ops-artifacts" {
  bucket = "readysettech-build-buildkite-ops-artifacts-us-east-2"
  tags = {
    Name = "readysettech-build-buildkite-ops-artifacts-us-east-2"
  }
  versioning {
    enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "ops-artifacts" {
  bucket = aws_s3_bucket.ops-artifacts.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Separate queue for running the ops pipelines, so that we can give admin permissions to only that role
module "buildkite_ops_queue" {
  source          = "../../../../../modules/buildkite-queue/regional"
  environment     = "build"
  buildkite_queue = "ops"
  instance_type   = "t3.large"

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  secrets_bucket   = aws_s3_bucket.ops-secrets.bucket
  artifacts_bucket = aws_s3_bucket.ops-artifacts.bucket
}
