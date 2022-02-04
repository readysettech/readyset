data "aws_subnets" "public_subnets" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  filter {
    name   = var.vpc_subnet_data_source_key
    values = var.vpc_public_subnet_data_value
  }
}

data "aws_subnets" "private_subnets" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  filter {
    name   = var.vpc_subnet_data_source_key
    values = var.vpc_private_subnet_data_value
  }
}
