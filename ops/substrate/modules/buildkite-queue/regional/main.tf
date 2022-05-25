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

    "IMDSv2Tokens" = "required"
    "VpcId"   = data.aws_vpc.vpc.id
    "Subnets" = local.subnet_ids
    },
    local.ssh_key_pair_config,
    local.agent_addtl_sudo_perm_config
  )
}
