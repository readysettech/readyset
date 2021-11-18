locals {
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
