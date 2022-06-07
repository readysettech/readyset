output "storage_integration_name" {
  description = "Name of the created storage integration"
  value       = snowflake_storage_integration.aws_s3.name
}
