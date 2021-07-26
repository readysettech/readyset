# managed by Substrate; do not edit by hand

resource "aws_internet_gateway" "admin-default-eu-west-1" {
  tags = {
    Environment      = "admin"
    Manager          = "Terraform"
    Name             = "admin-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.admin-default-eu-west-1.id
}

resource "aws_ram_resource_share" "admin-default-eu-west-1" {
  allow_external_principals = false
  name                      = "admin-default-eu-west-1"
  tags = {
    Environment      = "admin"
    Manager          = "Terraform"
    Name             = "admin-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_ram_principal_association" "admin-default-eu-west-1" {
  principal          = data.aws_organizations_organization.current.arn
  resource_share_arn = aws_ram_resource_share.admin-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "admin-default-public-eu-west-1a" {
  resource_arn       = aws_subnet.admin-default-public-eu-west-1a.arn
  resource_share_arn = aws_ram_resource_share.admin-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "admin-default-public-eu-west-1b" {
  resource_arn       = aws_subnet.admin-default-public-eu-west-1b.arn
  resource_share_arn = aws_ram_resource_share.admin-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "admin-default-public-eu-west-1c" {
  resource_arn       = aws_subnet.admin-default-public-eu-west-1c.arn
  resource_share_arn = aws_ram_resource_share.admin-default-eu-west-1.arn
}

resource "aws_route" "admin-default-public-internet-ipv4-eu-west-1" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.admin-default-eu-west-1.id
  route_table_id         = aws_vpc.admin-default-eu-west-1.default_route_table_id
}

resource "aws_route" "admin-default-public-internet-ipv6-eu-west-1" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.admin-default-eu-west-1.id
  route_table_id              = aws_vpc.admin-default-eu-west-1.default_route_table_id
}

resource "aws_route_table_association" "admin-default-public-eu-west-1a" {
  route_table_id = aws_vpc.admin-default-eu-west-1.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-eu-west-1a.id
}

resource "aws_route_table_association" "admin-default-public-eu-west-1b" {
  route_table_id = aws_vpc.admin-default-eu-west-1.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-eu-west-1b.id
}

resource "aws_route_table_association" "admin-default-public-eu-west-1c" {
  route_table_id = aws_vpc.admin-default-eu-west-1.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-eu-west-1c.id
}

resource "aws_subnet" "admin-default-public-eu-west-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1a"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-eu-west-1.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-eu-west-1.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "eu-west-1a"
    Connectivity     = "public"
    Environment      = "admin"
    Manager          = "Terraform"
    Name             = "admin-default-public-eu-west-1a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.admin-default-eu-west-1.id
}

resource "aws_subnet" "admin-default-public-eu-west-1b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1b"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-eu-west-1.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-eu-west-1.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "eu-west-1b"
    Connectivity     = "public"
    Environment      = "admin"
    Manager          = "Terraform"
    Name             = "admin-default-public-eu-west-1b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.admin-default-eu-west-1.id
}

resource "aws_subnet" "admin-default-public-eu-west-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1c"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-eu-west-1.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-eu-west-1.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "eu-west-1c"
    Connectivity     = "public"
    Environment      = "admin"
    Manager          = "Terraform"
    Name             = "admin-default-public-eu-west-1c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.admin-default-eu-west-1.id
}

resource "aws_vpc" "admin-default-eu-west-1" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "192.168.32.0/21"
  enable_dns_hostnames             = true
  enable_dns_support               = true
  tags = {
    Environment      = "admin"
    Manager          = "Terraform"
    Name             = "admin-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_vpc_endpoint" "admin-default-eu-west-1" {
  route_table_ids = [aws_vpc.admin-default-eu-west-1.default_route_table_id]
  service_name    = "com.amazonaws.eu-west-1.s3"
  tags = {
    Environment      = "admin"
    Manager          = "Terraform"
    Name             = "admin-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.admin-default-eu-west-1.id
}

data "aws_organizations_organization" "current" {
}
