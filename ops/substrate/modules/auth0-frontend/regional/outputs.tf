# Instance Profile
output "iam_instance_profile_name" {
  description = "Name of the IAM instance profile created for the Tailscale subnet router instance."
  value       = module.auth0-frontend-iam-role.iam_instance_profile_name
}
output "iam_instance_profile_arn" {
  description = "ARN of the IAM instance profile created for the Tailscale subnet router instance."
  value       = module.auth0-frontend-iam-role.iam_instance_profile_arn
}
# IAM Role
output "iam_role_name" {
  description = "Name of the IAM role created for the Tailscale subnet router instance."
  value       = module.auth0-frontend-iam-role.iam_role_name
}
output "iam_role_arn" {
  description = "ARN of the IAM role created for the Tailscale subnet router instance."
  value       = module.auth0-frontend-iam-role.iam_role_arn
}