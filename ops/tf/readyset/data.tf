data "aws_region" "current" {}

data "aws_vpc" "vpc" {
  tags = {
    Name = var.vpc
  }
}

data "aws_subnet_ids" "private" {
  count  = length(var.private_subnet_ids) == 0 ? 1 : 0
  vpc_id = data.aws_vpc.vpc.id

  filter {
    name   = format("tag:%s", var.subnet_tag)
    values = [var.private_subnet_tag]
  }
}

# AMIs
data "aws_ami" "mysql_adapter" {
  owners      = [local.ami_account_id]
  most_recent = true

  filter {
    name = "name"
    values = [
      format("readyset/images/hvm-ssd/readyset-mysql-adapter-%s-amd64-*", var.readyset_version)
    ]
  }
}

data "aws_ami" "server" {
  owners      = [local.ami_account_id]
  most_recent = true

  filter {
    name = "name"
    values = [
      format("readyset/images/hvm-ssd/readyset-server-%s-amd64-*", var.readyset_version)
    ]
  }
}

data "aws_ami" "zookeeper" {
  owners      = [local.ami_account_id]
  most_recent = true

  filter {
    name = "name"
    values = [
      format("readyset/images/hvm-ssd/readyset-zookeeper-%s-amd64-*", var.readyset_version)
    ]
  }
}
