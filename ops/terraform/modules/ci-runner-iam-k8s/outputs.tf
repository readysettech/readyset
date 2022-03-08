output "iam_role_arn" {
  description = "ARN of the IAM role created by the module."
  value       = module.cfn-iam-role.iam_role_arn
}

output "iam_role_name" {
  description = "Name of the IAM role created by the module."
  value       = module.cfn-iam-role.iam_role_name
}
