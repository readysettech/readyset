# managed by Substrate; do not edit by hand

resource "aws_ec2_tag" "readyset-sandbox-default-subnet-connectivity-eu-west-1" {
  for_each    = data.aws_subnet.readyset-sandbox-default-eu-west-1
  key         = "Connectivity"
  resource_id = each.value.id
  value       = each.value.tags["Connectivity"]
}


resource "aws_ec2_tag" "readyset-sandbox-default-subnet-environment-eu-west-1" {
  for_each    = data.aws_subnet.readyset-sandbox-default-eu-west-1
  key         = "Environment"
  resource_id = each.value.id
  value       = "sandbox"
}


resource "aws_ec2_tag" "readyset-sandbox-default-subnet-name-eu-west-1" {
  for_each    = data.aws_subnet.readyset-sandbox-default-eu-west-1
  key         = "Name"
  resource_id = each.value.id
  value       = "sandbox-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
}


resource "aws_ec2_tag" "readyset-sandbox-default-subnet-quality-eu-west-1" {
  for_each    = data.aws_subnet.readyset-sandbox-default-eu-west-1
  key         = "Quality"
  resource_id = each.value.id
  value       = "default"
}


resource "aws_ec2_tag" "readyset-sandbox-default-vpc-environment-eu-west-1" {
  key         = "Environment"
  resource_id = data.aws_vpc.readyset-sandbox-default-eu-west-1.id
  value       = "sandbox"
}


resource "aws_ec2_tag" "readyset-sandbox-default-vpc-name-eu-west-1" {
  key         = "Name"
  resource_id = data.aws_vpc.readyset-sandbox-default-eu-west-1.id
  value       = "sandbox-default"
}


resource "aws_ec2_tag" "readyset-sandbox-default-vpc-quality-eu-west-1" {
  key         = "Quality"
  resource_id = data.aws_vpc.readyset-sandbox-default-eu-west-1.id
  value       = "default"
}


resource "aws_ram_resource_share" "readyset-sandbox-default-eu-west-1" {
  allow_external_principals = false
  name                      = "readyset-sandbox-default-eu-west-1"
  provider                  = aws.network
  tags = {
    Environment = "sandbox"
    Name        = "readyset-sandbox-default"
    Quality     = "default"
  }
}

resource "aws_ram_principal_association" "readyset-sandbox-default-eu-west-1" {
  principal          = "069491470376"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.readyset-sandbox-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "readyset-sandbox-default-eu-west-1" {
  for_each           = data.aws_subnet.readyset-sandbox-default-eu-west-1
  provider           = aws.network
  resource_arn       = each.value.arn
  resource_share_arn = aws_ram_resource_share.readyset-sandbox-default-eu-west-1.arn
}

data "aws_subnet" "readyset-sandbox-default-eu-west-1" {
  for_each = data.aws_subnet_ids.readyset-sandbox-default-eu-west-1.ids
  id       = each.value
  provider = aws.network
}


data "aws_subnet_ids" "readyset-sandbox-default-eu-west-1" {
  provider = aws.network
  tags = {
    Environment = "sandbox"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.readyset-sandbox-default-eu-west-1.id
}


data "aws_vpc" "readyset-sandbox-default-eu-west-1" {
  provider = aws.network
  tags = {
    Environment = "sandbox"
    Quality     = "default"
  }
}

