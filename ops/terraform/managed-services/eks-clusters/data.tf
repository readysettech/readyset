#-------------- [ Networking ] -------------------------------------- #

data "aws_vpc" "network" {
  provider = aws.network
  id       = var.vpc_id
}

data "aws_subnet_ids" "public" {
  provider = aws.network
  vpc_id   = var.vpc_id
  filter {
    name   = "tag:Connectivity"
    values = ["public"]
  }
}

data "aws_subnet_ids" "private" {
  provider = aws.network
  vpc_id   = var.vpc_id
  filter {
    name   = "tag:Connectivity"
    values = ["private"]
  }
}
