output "vpc_security_group_id" {
  description = "ID of the VPC security group applied to subnet router instances."
  value       = aws_security_group.tailscale.id
}
#-------------- [ Networking ] ----------------------------------------- #

output "vpc_id" {
  description = "ID of the VPC to deploy Tailscale subnet router within."
  value       = var.vpc_id
}

output "eip_object" {
  description = "Elastic IP object associated with the Tailscale subnet router instance."
  value       = aws_eip.subnet-router
}

#-------------- [ Instance Profile ] ----------------------------------- #

output "iam_instance_profile_name" {
  description = "Name of the IAM instance profile created for the Tailscale subnet router instance."
  value       = module.subnet-router-iam-role.iam_instance_profile_name
}

output "iam_instance_profile_arn" {
  description = "ARN of the IAM instance profile created for the Tailscale subnet router instance."
  value       = module.subnet-router-iam-role.iam_instance_profile_arn
}

#-------------- [ IAM Role ] ------------------------------------------- #

output "iam_role_name" {
  description = "Name of the IAM role created for the Tailscale subnet router instance."
  value       = module.subnet-router-iam-role.iam_role_name
}

output "iam_role_arn" {
  description = "ARN of the IAM role created for the Tailscale subnet router instance."
  value       = module.subnet-router-iam-role.iam_role_arn
}
