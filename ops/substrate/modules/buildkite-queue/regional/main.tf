locals {
  stack_name = "buildkite-${var.buildkite_queue}"
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

resource "aws_cloudformation_stack" "main" {
  name = local.stack_name

  # TODO: Upgrade this to latest when possible
  template_url = "https://s3.amazonaws.com/buildkite-aws-stack/5.5.1/aws-stack.yml"

  capabilities = ["CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"]
  parameters = {
    "ArtifactsBucket" = var.artifacts_bucket
    "SecretsBucket"   = var.secrets_bucket

    "BuildkiteQueue" = var.buildkite_queue

    "BuildkiteAgentTokenParameterStorePath" = var.buildkite_agent_token_parameter_store_path

    "InstanceType" = var.instance_type

    "ECRAccessPolicy" = "poweruser"

    "MaxSize" = var.max_size
    "MinSize" = var.min_size

    "VpcId"   = data.aws_vpc.vpc.id
    "Subnets" = join(",", data.aws_subnets.public_subnets.ids)
  }
}

