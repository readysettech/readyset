output "secrets_bucket" {
  value = local.secrets_bucket_name
}

output "artifacts_bucket" {
  value = local.artifacts_bucket_name
}

output "buildkite_agent_token_parameter_store_path" {
  value = local.buildkite_agent_token_parameter_store_path
}
