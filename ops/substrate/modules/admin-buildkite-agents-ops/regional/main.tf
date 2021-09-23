data "aws_region" "current" {}

data "aws_vpc" "vpc" {
  tags = {
    Name = "admin-default"
  }
}

# TODO: Figure out why we only have public subnets in admin, if moving to
# a private subnet makes sense, and if so how.
data "aws_subnets" "public_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.vpc.id]
  }

  filter {
    name   = "tag:Connectivity"
    values = ["public"]
  }
}
