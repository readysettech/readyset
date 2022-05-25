# managed by Substrate; do not edit by hand

data "aws_subnet" "admin-default-sa-east-1" {
  for_each = data.aws_subnet_ids.admin-default-sa-east-1.ids
  id       = each.value
  provider = aws.network
}

data "aws_subnet_ids" "admin-default-sa-east-1" {
  provider = aws.network
  tags = {
    Environment = "admin"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.admin-default-sa-east-1.id
}

data "aws_vpc" "admin-default-sa-east-1" {
  provider = aws.network
  tags = {
    Environment = "admin"
    Quality     = "default"
  }
}

resource "aws_ec2_tag" "admin-default-subnet-connectivity-sa-east-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.admin-default-sa-east-1
  key         = "Connectivity"
  resource_id = each.value.id
  value       = each.value.tags["Connectivity"]
}


resource "aws_ec2_tag" "admin-default-subnet-environment-sa-east-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.admin-default-sa-east-1
  key         = "Environment"
  resource_id = each.value.id
  value       = "admin"
}


resource "aws_ec2_tag" "admin-default-subnet-name-sa-east-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.admin-default-sa-east-1
  key         = "Name"
  resource_id = each.value.id
  value       = "admin-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
}


resource "aws_ec2_tag" "admin-default-subnet-quality-sa-east-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.admin-default-sa-east-1
  key         = "Quality"
  resource_id = each.value.id
  value       = "default"
}


resource "aws_ec2_tag" "admin-default-vpc-environment-sa-east-1" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Environment"
  resource_id = data.aws_vpc.admin-default-sa-east-1.id
  value       = "admin"
}


resource "aws_ec2_tag" "admin-default-vpc-name-sa-east-1" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Name"
  resource_id = data.aws_vpc.admin-default-sa-east-1.id
  value       = "admin-default"
}


resource "aws_ec2_tag" "admin-default-vpc-quality-sa-east-1" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Quality"
  resource_id = data.aws_vpc.admin-default-sa-east-1.id
  value       = "default"
}


resource "aws_ram_principal_association" "admin-default-sa-east-1" {
  principal          = "716876017850"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.admin-default-sa-east-1.arn
}

resource "aws_ram_resource_association" "admin-default-sa-east-1" {
  for_each           = data.aws_subnet.admin-default-sa-east-1
  provider           = aws.network
  resource_arn       = each.value.arn
  resource_share_arn = aws_ram_resource_share.admin-default-sa-east-1.arn
}

resource "aws_ram_resource_share" "admin-default-sa-east-1" {
  allow_external_principals = false
  name                      = "admin-default-sa-east-1"
  provider                  = aws.network
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
}

resource "time_sleep" "share-before-tag" {
  create_duration = "60s"
  depends_on      = [aws_ram_resource_association.admin-default-sa-east-1]
}

