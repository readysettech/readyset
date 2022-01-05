locals {
  stack_name    = "buildkite-${var.buildkite_queue}"
  stack_version = "master"
}

data "aws_vpc" "vpc" {
  tags = {
    Name = "${var.environment}-default"
  }
}

data "aws_subnets" "public_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.vpc.id]
  }

  filter {
    name   = "tag:Connectivity"
    values = ["public"]
  }
}

data "aws_subnets" "private_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.vpc.id]
  }

  filter {
    name   = "tag:Connectivity"
    values = ["private"]
  }
}

data "aws_s3_bucket" "secrets_bucket" {
  bucket = var.secrets_bucket
}

resource "aws_cloudformation_stack" "main" {
  name = local.stack_name

  template_url = "https://s3.amazonaws.com/buildkite-aws-stack/${local.stack_version}/aws-stack.yml"

  capabilities = ["CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"]
  parameters = merge({
    "ArtifactsBucket"     = var.artifacts_bucket
    "SecretsBucket"       = var.secrets_bucket
    "SecretsBucketRegion" = data.aws_s3_bucket.secrets_bucket.region

    "BuildkiteQueue" = var.buildkite_queue

    "BuildkiteAgentTokenParameterStorePath" = var.buildkite_agent_token_parameter_store_path

    "InstanceType" = var.instance_type

    "ECRAccessPolicy"  = "poweruser"
    "ManagedPolicyARN" = join(",", var.extra_iam_policy_arns)

    "MaxSize" = var.max_size
    "MinSize" = var.min_size

    "VpcId"   = data.aws_vpc.vpc.id
    "Subnets" = local.subnet_ids
  },
  local.ssh_key_pair_config)
}

data "aws_iam_roles" "iam_roles" {
  name_regex = "${local.stack_name}-.*"
  depends_on = [aws_cloudformation_stack.main]
}
