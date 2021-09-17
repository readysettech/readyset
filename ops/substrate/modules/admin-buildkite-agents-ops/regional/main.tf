data "aws_region" "current" {}

data "aws_vpc" "vpc" {
  tags = {
    Name = "admin-default"
  }
}

# TODO: Figure out why we only have public subnets in admin, if moving to
# a private subnet makes sense, and if so how.
data "aws_subnets" "public_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.vpc.id]
  }

  filter {
    name = "tag:Connectivity"
    values = ["public"]
  }
}
resource "aws_cloudformation_stack" "buildkite_ops" {
  name = "buildkite-ops"

  template_url = "https://s3.amazonaws.com/buildkite-aws-stack/latest/aws-stack.yml"

  capabilities = [ "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND" ]
  parameters = {
    "ArtifactsBucket" = "readysettech-admin-buildkite-ops-artifacts"
    # There is no resource we can pull this from as this path comes through the
    # paramater store secret manager integration
    # https://docs.aws.amazon.com/systems-manager/latest/userguide/integration-ps-secretsmanager.html
    "BuildkiteAgentTokenParameterStorePath" = "/aws/reference/secretsmanager/buildkite-agent-token"
    "BuildkiteQueue" = "ops"
    # Running on the default instance type as this should not be heavyweight.
    "InstanceType" = "t3.large"
    "VpcId" = data.aws_vpc.vpc.id
    "Subnets" = join(",", data.aws_subnets.public_subnets.ids)
  }
}

data "aws_iam_policy_document" "buildkite_agent_token" {
  statement {
    effect = "Allow"
    principals {
      type = "AWS"
      identifiers = [
        // TODO: Get these from the cloud formation stack
        "arn:aws:iam::716876017850:role/buildkite-ops-AutoscalingLambdaExecutionRole-FE4B6BI48QFW",
        "arn:aws:iam::716876017850:role/buildkite-ops-Role"
      ]
    }
    actions = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}
resource "aws_secretsmanager_secret" "buildkite_agent_token" {
  # We only want to set up the secrets manager in us-east-2 and replicate to us-west-2
  count = data.aws_region.current.name == "us-east-2" ? 1 : 0
  name = "buildkite-agent-token"

  policy = data.aws_iam_policy_document.buildkite_agent_token.json
  replica {
    region = "us-west-2"
  }
}
