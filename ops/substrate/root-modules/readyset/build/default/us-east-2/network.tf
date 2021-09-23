# managed by Substrate; do not edit by hand

resource "aws_ec2_tag" "readyset-build-default-subnet-connectivity-us-east-2" {
  for_each    = data.aws_subnet.readyset-build-default-us-east-2
  key         = "Connectivity"
  resource_id = each.value.id
  value       = each.value.tags["Connectivity"]
}


resource "aws_ec2_tag" "readyset-build-default-subnet-environment-us-east-2" {
  for_each    = data.aws_subnet.readyset-build-default-us-east-2
  key         = "Environment"
  resource_id = each.value.id
  value       = "build"
}


resource "aws_ec2_tag" "readyset-build-default-subnet-name-us-east-2" {
  for_each    = data.aws_subnet.readyset-build-default-us-east-2
  key         = "Name"
  resource_id = each.value.id
  value       = "build-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
}


resource "aws_ec2_tag" "readyset-build-default-subnet-quality-us-east-2" {
  for_each    = data.aws_subnet.readyset-build-default-us-east-2
  key         = "Quality"
  resource_id = each.value.id
  value       = "default"
}


resource "aws_ec2_tag" "readyset-build-default-vpc-environment-us-east-2" {
  key         = "Environment"
  resource_id = data.aws_vpc.readyset-build-default-us-east-2.id
  value       = "build"
}


resource "aws_ec2_tag" "readyset-build-default-vpc-name-us-east-2" {
  key         = "Name"
  resource_id = data.aws_vpc.readyset-build-default-us-east-2.id
  value       = "build-default"
}


resource "aws_ec2_tag" "readyset-build-default-vpc-quality-us-east-2" {
  key         = "Quality"
  resource_id = data.aws_vpc.readyset-build-default-us-east-2.id
  value       = "default"
}


resource "aws_ram_resource_share" "readyset-build-default-us-east-2" {
  allow_external_principals = false
  name                      = "readyset-build-default-us-east-2"
  provider                  = aws.network
  tags = {
    Environment = "build"
    Name        = "readyset-build-default"
    Quality     = "default"
  }
}

resource "aws_ram_principal_association" "readyset-build-default-us-east-2" {
  principal          = "305232526136"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.readyset-build-default-us-east-2.arn
}

resource "aws_ram_resource_association" "readyset-build-default-us-east-2" {
  for_each           = data.aws_subnet.readyset-build-default-us-east-2
  provider           = aws.network
  resource_arn       = each.value.arn
  resource_share_arn = aws_ram_resource_share.readyset-build-default-us-east-2.arn
}

data "aws_subnet" "readyset-build-default-us-east-2" {
  for_each = data.aws_subnet_ids.readyset-build-default-us-east-2.ids
  id       = each.value
  provider = aws.network
}


data "aws_subnet_ids" "readyset-build-default-us-east-2" {
  provider = aws.network
  tags = {
    Environment = "build"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.readyset-build-default-us-east-2.id
}


data "aws_vpc" "readyset-build-default-us-east-2" {
  provider = aws.network
  tags = {
    Environment = "build"
    Quality     = "default"
  }
}

