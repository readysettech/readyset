locals {
  instance_types = [
    "t3a.small",
    "r5a.2xlarge",
    "c5a.4xlarge",
  ]

  extra_iam_policy_arns = [
    module.buildkite_queue_shared.packer_policy_arn,
    module.buildkite_queue_shared.metadata_bucket_policy_arn,
  ]
}

module "buildkite_queue_shared" {
  source           = "../../../../../modules/buildkite-queue-shared/regional"
  environment      = "build"
  buildkite_queues = [for it in local.instance_types : replace(it, ".", "-")]

  agent_token_allowed_roles = concat(
    tolist(module.buildkite_ops_queue.iam_roles),
    tolist(module.buildkite_default_queue.iam_roles),
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

  extra_iam_policy_arns = local.extra_iam_policy_arns

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

# Separate module for the default queue, so we can name the queue "default"
module "buildkite_default_queue" {
  source          = "../../../../../modules/buildkite-queue/regional"
  environment     = "build"
  buildkite_queue = "default"
  instance_type   = "c5a.2xlarge"

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  extra_iam_policy_arns = local.extra_iam_policy_arns

  secrets_bucket   = aws_s3_bucket.ops-secrets.bucket
  artifacts_bucket = aws_s3_bucket.ops-artifacts.bucket
}

# Separate queue for running the ops pipelines, so that we can give admin permissions to only that role
module "buildkite_ops_queue" {
  source          = "../../../../../modules/buildkite-queue/regional"
  environment     = "build"
  buildkite_queue = "ops"
  instance_type   = "t3.large"

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  extra_iam_policy_arns = local.extra_iam_policy_arns

  secrets_bucket   = aws_s3_bucket.ops-secrets.bucket
  artifacts_bucket = aws_s3_bucket.ops-artifacts.bucket
}
