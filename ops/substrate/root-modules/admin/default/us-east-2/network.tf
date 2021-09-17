# managed by Substrate; do not edit by hand

resource "aws_ec2_tag" "admin-default-subnet-connectivity-us-east-2" {
  for_each    = data.aws_subnet.admin-default-us-east-2
  key         = "Connectivity"
  resource_id = each.value.id
  value       = each.value.tags["Connectivity"]
}


resource "aws_ec2_tag" "admin-default-subnet-environment-us-east-2" {
  for_each    = data.aws_subnet.admin-default-us-east-2
  key         = "Environment"
  resource_id = each.value.id
  value       = "admin"
}


resource "aws_ec2_tag" "admin-default-subnet-name-us-east-2" {
  for_each    = data.aws_subnet.admin-default-us-east-2
  key         = "Name"
  resource_id = each.value.id
  value       = "admin-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
}


resource "aws_ec2_tag" "admin-default-subnet-quality-us-east-2" {
  for_each    = data.aws_subnet.admin-default-us-east-2
  key         = "Quality"
  resource_id = each.value.id
  value       = "default"
}


resource "aws_ec2_tag" "admin-default-vpc-environment-us-east-2" {
  key         = "Environment"
  resource_id = data.aws_vpc.admin-default-us-east-2.id
  value       = "admin"
}


resource "aws_ec2_tag" "admin-default-vpc-name-us-east-2" {
  key         = "Name"
  resource_id = data.aws_vpc.admin-default-us-east-2.id
  value       = "admin-default"
}


resource "aws_ec2_tag" "admin-default-vpc-quality-us-east-2" {
  key         = "Quality"
  resource_id = data.aws_vpc.admin-default-us-east-2.id
  value       = "default"
}


resource "aws_ram_resource_share" "admin-default-us-east-2" {
  allow_external_principals = false
  name                      = "admin-default-us-east-2"
  provider                  = aws.network
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
}

resource "aws_ram_principal_association" "admin-default-us-east-2" {
  principal          = "716876017850"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.admin-default-us-east-2.arn
}

resource "aws_ram_resource_association" "admin-default-us-east-2" {
  for_each           = data.aws_subnet.admin-default-us-east-2
  provider           = aws.network
  resource_arn       = each.value.arn
  resource_share_arn = aws_ram_resource_share.admin-default-us-east-2.arn
}

data "aws_subnet" "admin-default-us-east-2" {
  for_each = data.aws_subnet_ids.admin-default-us-east-2.ids
  id       = each.value
  provider = aws.network
}


data "aws_subnet_ids" "admin-default-us-east-2" {
  provider = aws.network
  tags = {
    Environment = "admin"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.admin-default-us-east-2.id
}


data "aws_vpc" "admin-default-us-east-2" {
  provider = aws.network
  tags = {
    Environment = "admin"
    Quality     = "default"
  }
}

