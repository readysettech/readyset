output "secrets_bucket" {
  value = local.secrets_bucket_name
}

output "artifacts_bucket" {
  value = local.artifacts_bucket_name
}

output "metadata_bucket" {
  value = local.metadata_bucket_name
}

output "buildkite_agent_token_parameter_store_path" {
  value = local.buildkite_agent_token_parameter_store_path
}

output "packer_policy_arn" {
  value = aws_iam_policy.packer_policy.arn
}

output "metadata_bucket_policy_arn" {
  value = aws_iam_policy.metadata_bucket_policy.arn
}

output "cache_buckets_policy_arn" {
  value = aws_iam_policy.cache_buckets_policy.arn
}
