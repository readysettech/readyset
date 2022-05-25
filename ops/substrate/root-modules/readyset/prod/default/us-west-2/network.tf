# managed by Substrate; do not edit by hand

resource "aws_ec2_tag" "readyset-prod-default-subnet-connectivity-us-west-2" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-us-west-2
  key         = "Connectivity"
  resource_id = each.value.id
  value       = each.value.tags["Connectivity"]
}


resource "aws_ec2_tag" "readyset-prod-default-subnet-environment-us-west-2" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-us-west-2
  key         = "Environment"
  resource_id = each.value.id
  value       = "prod"
}


resource "aws_ec2_tag" "readyset-prod-default-subnet-name-us-west-2" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-us-west-2
  key         = "Name"
  resource_id = each.value.id
  value       = "prod-default-${each.value.tags["Connectivity"]}-${each.value.availability_zone}"
}


resource "aws_ec2_tag" "readyset-prod-default-subnet-quality-us-west-2" {
  depends_on  = [time_sleep.share-before-tag]
  for_each    = data.aws_subnet.readyset-prod-default-us-west-2
  key         = "Quality"
  resource_id = each.value.id
  value       = "default"
}


resource "aws_ec2_tag" "readyset-prod-default-vpc-environment-us-west-2" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Environment"
  resource_id = data.aws_vpc.readyset-prod-default-us-west-2.id
  value       = "prod"
}


resource "aws_ec2_tag" "readyset-prod-default-vpc-name-us-west-2" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Name"
  resource_id = data.aws_vpc.readyset-prod-default-us-west-2.id
  value       = "prod-default"
}


resource "aws_ec2_tag" "readyset-prod-default-vpc-quality-us-west-2" {
  depends_on  = [time_sleep.share-before-tag]
  key         = "Quality"
  resource_id = data.aws_vpc.readyset-prod-default-us-west-2.id
  value       = "default"
}


resource "aws_ram_principal_association" "readyset-prod-default-us-west-2" {
  principal          = "431238456211"
  provider           = aws.network
  resource_share_arn = aws_ram_resource_share.readyset-prod-default-us-west-2.arn
}

resource "aws_ram_resource_association" "readyset-prod-default-us-west-2" {
  for_each           = data.aws_subnet.readyset-prod-default-us-west-2
  provider           = aws.network
  resource_arn       = each.value.arn
  resource_share_arn = aws_ram_resource_share.readyset-prod-default-us-west-2.arn
}

resource "aws_ram_resource_share" "readyset-prod-default-us-west-2" {
  allow_external_principals = false
  name                      = "readyset-prod-default-us-west-2"
  provider                  = aws.network
  tags = {
    Environment = "prod"
    Name        = "readyset-prod-default"
    Quality     = "default"
  }
}

data "aws_subnet" "readyset-prod-default-us-west-2" {
  for_each = data.aws_subnet_ids.readyset-prod-default-us-west-2.ids
  id       = each.value
  provider = aws.network
}

data "aws_subnet_ids" "readyset-prod-default-us-west-2" {
  provider = aws.network
  tags = {
    Environment = "prod"
    Quality     = "default"
  }
  vpc_id = data.aws_vpc.readyset-prod-default-us-west-2.id
}

data "aws_vpc" "readyset-prod-default-us-west-2" {
  provider = aws.network
  tags = {
    Environment = "prod"
    Quality     = "default"
  }
}

resource "time_sleep" "share-before-tag" {
  create_duration = "60s"
  depends_on      = [aws_ram_resource_association.readyset-prod-default-us-west-2]
}

