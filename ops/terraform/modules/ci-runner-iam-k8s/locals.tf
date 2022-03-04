locals {
  env_region               = format("%s-%s", var.environment, data.aws_region.current.name)
  iam_role_name            = format("readyset-ci-k8s-%s", local.env_region)
  iam_role_assume_pol_name = format("readyset-ci-k8s-assume-%s", local.env_region)
  iam_pol_name             = format("readyset-ci-k8s-%s", local.env_region)
  # AWS accounts to entrust with federation capabilities
  iam_trusted_role_arns = flatten([
    var.iam_role_trusted_account_ids,
    [data.aws_caller_identity.current.account_id]
  ])
  # Any policies to be granted to the IAM role
  iam_role_custom_policy_arns = [
    aws_iam_policy.this-cfn.arn
  ]
}
