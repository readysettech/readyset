locals {
  effective_ami_id        = length(var.ami_name_filter) > 0 ? data.aws_ami.latest-ts.0.id : var.ami_id
  secrets_manager_kms_key = length(var.iam_authorized_secrets_manager_kms_key_arn) == 0 ? data.aws_kms_key.default-sm.arn : var.iam_authorized_secrets_manager_kms_key_arn
  subnet_router_hostname  = format("tailscale-%s-%s-%s", var.environment, var.quality, var.aws_region)
  subnet_router_iam_role_name = format("tailscale-subnet-router-role-%s-%s-%s",
    var.quality,
    var.environment,
    var.aws_region
  )
  subnet_router_iam_policy_name = format("tailscale-subnet-router-iam-%s-%s-%s",
    var.quality,
    var.environment,
    var.aws_region
  )
  subnet_router_ec2_name = format("tailscale-subnet-router-%s-%s-%s",
    var.quality,
    var.environment,
    var.aws_region
  )
  subnet_router_vpc_sg_name = format("tailscale-subnet-router-sg-%s-%s",
    var.quality,
    var.environment
  )
}
