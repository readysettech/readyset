module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> v2.0"

  name            = "readyset-vpc-${local.random}"
  cidr            = var.vpc_cidr
  azs             = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets = var.vpc_private_subnets
  public_subnets  = var.vpc_public_subnets

  # Deploy only one NAT gateway (To save $)
  enable_nat_gateway     = true
  single_nat_gateway     = true
  one_nat_gateway_per_az = false

  # Configure public access to RDS instances
  create_database_subnet_group           = true
  create_database_subnet_route_table     = true
  create_database_internet_gateway_route = true
  enable_dns_hostnames                   = true
  enable_dns_support                     = true

  tags = {
    Name        = "readyset-vpc-${local.random}"
    Terraform   = "true"
    Environment = "dev"
    Owner       = "Readyset"
  }
}
