module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> v2.0"

  name = format("readyset-vpc-%s", local.random)
  cidr = var.vpc_cidr_block
  # TODO: This should be a map of Availability Zones for supported regions
  #       or a list provided by the consumer. Not all regions have 3 
  #       availability zones nor use 'a' as the base index. This does not 
  #       affect the current application of the module but could break in 
  #       future instances.
  azs             = formatlist("%s%s", var.aws_region, ["a", "b", "c"])
  private_subnets = var.vpc_private_cidr_blocks
  public_subnets  = var.vpc_public_cidr_blocks

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
    Name        = format("readyset-vpc-%s", local.random)
    Terraform   = "true"
    Environment = "dev"
    Owner       = "Readyset"
  }
}
