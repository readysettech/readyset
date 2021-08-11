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
  value       = "build"
}


resource "aws_ec2_tag" "subnet-name" {
  for_each    = data.aws_subnet.network
  key         = "Name"
  resource_id = each.value.id
  value       = "build-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
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
  value       = "build"
}


resource "aws_ec2_tag" "vpc-name" {
  key         = "Name"
  resource_id = data.aws_vpc.network.id
  value       = "build-default"
}


resource "aws_ec2_tag" "vpc-quality" {
  key         = "Quality"
  resource_id = data.aws_vpc.network.id
  value       = "default"
}


resource "aws_ram_resource_share" "readyset-build-default" {
  allow_external_principals = false
  name                      = "readyset-build-default"
  provider                  = aws.network
  tags = {
    Environment      = "build"
    Manager          = "Terraform"
    Name             = "readyset-build-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_ram_principal_association" "ready-build-default" {
  principal          = "305232526136"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.readyset-build-default.arn
}

data "aws_subnet" "network" {
  for_each = data.aws_subnet_ids.network.ids
  id       = each.value
  provider = aws.network
}


data "aws_subnet_ids" "network" {
  provider = aws.network
  tags = {
    Environment = "build"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.network.id
}


data "aws_vpc" "network" {
  provider = aws.network
  tags = {
    Environment = "build"
    Quality     = "default"
  }
}

