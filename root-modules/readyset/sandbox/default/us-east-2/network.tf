# managed by Substrate; do not edit by hand

resource "aws_ec2_tag" "subnet-connectivity" {
  for_each    = data.aws_subnet.network
  key         = "Connectivity"
  resource_id = each.value.id
  value       = each.value.tags["Connectivity"]
}


resource "aws_ec2_tag" "subnet-environment" {
  for_each    = data.aws_subnet.network
  key         = "Environment"
  resource_id = each.value.id
  value       = "sandbox"
}


resource "aws_ec2_tag" "subnet-name" {
  for_each    = data.aws_subnet.network
  key         = "Name"
  resource_id = each.value.id
  value       = "sandbox-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
}


resource "aws_ec2_tag" "subnet-quality" {
  for_each    = data.aws_subnet.network
  key         = "Quality"
  resource_id = each.value.id
  value       = "default"
}


resource "aws_ec2_tag" "vpc-environment" {
  key         = "Environment"
  resource_id = data.aws_vpc.network.id
  value       = "sandbox"
}


resource "aws_ec2_tag" "vpc-name" {
  key         = "Name"
  resource_id = data.aws_vpc.network.id
  value       = "sandbox-default"
}


resource "aws_ec2_tag" "vpc-quality" {
  key         = "Quality"
  resource_id = data.aws_vpc.network.id
  value       = "default"
}


resource "aws_ram_resource_share" "readyset-sandbox-default" {
  allow_external_principals = false
  name                      = "readyset-sandbox-default"
  provider                  = aws.network
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "readyset-sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_ram_principal_association" "ready-sandbox-default" {
  principal          = "069491470376"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.readyset-sandbox-default.arn
}

data "aws_subnet" "network" {
  for_each = data.aws_subnet_ids.network.ids
  id       = each.value
  provider = aws.network
}


data "aws_subnet_ids" "network" {
  provider = aws.network
  tags = {
    Environment = "sandbox"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.network.id
}


data "aws_vpc" "network" {
  provider = aws.network
  tags = {
    Environment = "sandbox"
    Quality     = "default"
  }
}

