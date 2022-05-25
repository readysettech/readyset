locals {
  env_region                   = format("%s-%s", var.environment, data.aws_region.current.name)
  devops_assets_s3_bucket_name = format("readyset-devops-assets-%s", local.env_region)
  # Benchmarking
  benchmarking_iam_role_name = format("readyset-ci-benchmarking-%s", local.env_region)
  benchmarking_iam_pol_name  = format("readyset-ci-benchmarking-pol-%s", local.env_region)
  benchmarking_iam_trusted_role_arns = var.benchmarking_iam_role_enabled ? flatten([
    var.benchmarking_iam_role_trusted_account_ids,
    [data.aws_caller_identity.current.account_id]
  ]) : null
  benchmarking_iam_role_custom_policy_arns = var.benchmarking_iam_role_enabled ? [
    aws_iam_policy.ci-benchmarking[0].arn
  ] : null
  benchmarking_s3_buckets_allowed = var.devops_assets_s3_bucket_enabled ? [
    aws_s3_bucket.devops-assets[0].id
  ] : null
  # Buildkite
  bk_benchmarking_iam_pol_name = format("readyset-benchmarking-assume-pol-%s", local.env_region)
  bk_k8s_role_iam_pol_name     = format("readyset-k8s-assume-pol-%s", local.env_region)
}
