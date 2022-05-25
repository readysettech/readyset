# managed by Substrate; do not edit by hand

resource "aws_ec2_tag" "readyset-prod-default-subnet-connectivity-ap-northeast-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-ap-northeast-1
  key         = "Connectivity"
  resource_id = each.value.id
  value       = each.value.tags["Connectivity"]
}


resource "aws_ec2_tag" "readyset-prod-default-subnet-environment-ap-northeast-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-ap-northeast-1
  key         = "Environment"
  resource_id = each.value.id
  value       = "prod"
}


resource "aws_ec2_tag" "readyset-prod-default-subnet-name-ap-northeast-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-ap-northeast-1
  key         = "Name"
  resource_id = each.value.id
  value       = "prod-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
}


resource "aws_ec2_tag" "readyset-prod-default-subnet-quality-ap-northeast-1" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-ap-northeast-1
  key         = "Quality"
  resource_id = each.value.id
  value       = "default"
}


resource "aws_ec2_tag" "readyset-prod-default-vpc-environment-ap-northeast-1" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Environment"
  resource_id = data.aws_vpc.readyset-prod-default-ap-northeast-1.id
  value       = "prod"
}


resource "aws_ec2_tag" "readyset-prod-default-vpc-name-ap-northeast-1" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Name"
  resource_id = data.aws_vpc.readyset-prod-default-ap-northeast-1.id
  value       = "prod-default"
}


resource "aws_ec2_tag" "readyset-prod-default-vpc-quality-ap-northeast-1" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Quality"
  resource_id = data.aws_vpc.readyset-prod-default-ap-northeast-1.id
  value       = "default"
}


resource "aws_ram_principal_association" "readyset-prod-default-ap-northeast-1" {
  principal          = "431238456211"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.readyset-prod-default-ap-northeast-1.arn
}

resource "aws_ram_resource_association" "readyset-prod-default-ap-northeast-1" {
  for_each           = data.aws_subnet.readyset-prod-default-ap-northeast-1
  provider           = aws.network
  resource_arn       = each.value.arn
  resource_share_arn = aws_ram_resource_share.readyset-prod-default-ap-northeast-1.arn
}

resource "aws_ram_resource_share" "readyset-prod-default-ap-northeast-1" {
  allow_external_principals = false
  name                      = "readyset-prod-default-ap-northeast-1"
  provider                  = aws.network
  tags = {
    Environment = "prod"
    Name        = "readyset-prod-default"
    Quality     = "default"
  }
}

data "aws_subnet" "readyset-prod-default-ap-northeast-1" {
  for_each = data.aws_subnet_ids.readyset-prod-default-ap-northeast-1.ids
  id       = each.value
  provider = aws.network
}

data "aws_subnet_ids" "readyset-prod-default-ap-northeast-1" {
  provider = aws.network
  tags = {
    Environment = "prod"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.readyset-prod-default-ap-northeast-1.id
}

data "aws_vpc" "readyset-prod-default-ap-northeast-1" {
  provider = aws.network
  tags = {
    Environment = "prod"
    Quality     = "default"
  }
}

resource "time_sleep" "share-before-tag" {
  create_duration = "60s"
  depends_on      = [aws_ram_resource_association.readyset-prod-default-ap-northeast-1]
}

