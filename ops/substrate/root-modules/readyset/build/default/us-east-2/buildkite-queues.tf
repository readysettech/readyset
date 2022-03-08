locals {
  instance_types = {
    "t3a.small"   = 10,
    "r5a.2xlarge" = 3,
    "c5a.4xlarge" = 3,
  }

  extra_iam_policy_arns = [
    module.buildkite_queue_shared.packer_policy_arn,
    module.buildkite_queue_shared.metadata_bucket_policy_arn,
    module.buildkite_queue_shared.cache_buckets_policy_arn,
  ]
}

resource "aws_s3_bucket" "sccache" {
  bucket = "readysettech-build-sccache-us-east-2"
  tags = {
    Name = "readysettech-build-sccache-us-east-2"
  }

  # No versioning as this is a cache. Probably should also have a lifecycle
  # policy at some point.
}

resource "aws_s3_bucket_public_access_block" "sccache" {
  bucket = aws_s3_bucket.sccache.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

module "buildkite_queue_shared" {
  source           = "../../../../../modules/buildkite-queue-shared/regional"
  environment      = "build"
  buildkite_queues = [for it in local.instance_types : replace(it, ".", "-")]

  agent_token_allowed_roles = concat(
    tolist(module.buildkite_ops_queue.iam_roles),
    tolist(module.buildkite_default_queue.iam_roles),
    tolist(module.buildkite_benchmark_queue.iam_roles),
    tolist(module.buildkite_k8s_queue.iam_roles),
    flatten(values(module.buildkite_queue)[*].iam_roles),
  )
}

module "buildkite_queue" {
  for_each = local.instance_types

  source      = "../../../../../modules/buildkite-queue/regional"
  environment = "build"

  buildkite_queue = replace(each.key, ".", "-")

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  secrets_bucket   = module.buildkite_queue_shared.secrets_bucket
  artifacts_bucket = module.buildkite_queue_shared.artifacts_bucket

  extra_iam_policy_arns = local.extra_iam_policy_arns

  instance_type = each.key
  max_size      = each.value
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

  max_size = 20

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
  max_size        = 15

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  extra_iam_policy_arns = local.extra_iam_policy_arns

  secrets_bucket   = aws_s3_bucket.ops-secrets.bucket
  artifacts_bucket = aws_s3_bucket.ops-artifacts.bucket
}

module "buildkite_benchmark_queue" {
  source          = "../../../../../modules/buildkite-queue/regional"
  environment     = "build"
  buildkite_queue = "benchmarks"
  instance_type   = "c5.4xlarge"

  min_size = 0
  max_size = 15

  ssh_key_pair_name                          = "readyset-devops"
  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path
  extra_iam_policy_arns = concat(
    local.extra_iam_policy_arns, [
      aws_iam_policy.bk-benchmarking-assume-role[0].arn
    ]
  )
  secrets_bucket   = aws_s3_bucket.ops-secrets.bucket
  artifacts_bucket = aws_s3_bucket.ops-artifacts.bucket
  # Needed to change file permissions during builds
  agent_additional_sudo_permissions = ["NOPASSWD:ALL"]
}

# Buildkite queue that will provide access to build k8s cluster
# This will live in private subnets and can therefore hit k8s
module "buildkite_k8s_queue" {
  source          = "../../../../../modules/buildkite-queue/regional"
  environment     = "build"
  buildkite_queue = "buildk8s"
  instance_type   = "c5.xlarge"

  min_size = 0
  max_size = 10
  ssh_key_pair_name                          = "readyset-devops"
  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path
  extra_iam_policy_arns = concat(
    local.extra_iam_policy_arns, [
      aws_iam_policy.bk-k8s-assume-role[0].arn
    ]
  )
  secrets_bucket   = aws_s3_bucket.ops-secrets.bucket
  artifacts_bucket = aws_s3_bucket.ops-artifacts.bucket
  # Needed to change file permissions during builds
  agent_additional_sudo_permissions = ["NOPASSWD:ALL"]
}
