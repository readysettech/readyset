# managed by Substrate; do not edit by hand

resource "aws_internet_gateway" "admin-default-sa-east-1" {
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.admin-default-sa-east-1.id
}

resource "aws_route" "admin-default-public-internet-ipv4-sa-east-1" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.admin-default-sa-east-1.id
  route_table_id         = aws_vpc.admin-default-sa-east-1.default_route_table_id
}

resource "aws_route" "admin-default-public-internet-ipv6-sa-east-1" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.admin-default-sa-east-1.id
  route_table_id              = aws_vpc.admin-default-sa-east-1.default_route_table_id
}

resource "aws_route_table_association" "admin-default-public-sa-east-1a" {
  route_table_id = aws_vpc.admin-default-sa-east-1.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-sa-east-1a.id
}

resource "aws_route_table_association" "admin-default-public-sa-east-1b" {
  route_table_id = aws_vpc.admin-default-sa-east-1.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-sa-east-1b.id
}

resource "aws_route_table_association" "admin-default-public-sa-east-1c" {
  route_table_id = aws_vpc.admin-default-sa-east-1.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-sa-east-1c.id
}

resource "aws_subnet" "admin-default-public-sa-east-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1a"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-sa-east-1.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-sa-east-1.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "sa-east-1a"
    Connectivity     = "public"
    Environment      = "admin"
    Name             = "admin-default-public-sa-east-1a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.admin-default-sa-east-1.id
}

resource "aws_subnet" "admin-default-public-sa-east-1b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1b"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-sa-east-1.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-sa-east-1.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "sa-east-1b"
    Connectivity     = "public"
    Environment      = "admin"
    Name             = "admin-default-public-sa-east-1b"
    Quality          = "default"
  }
  vpc_id = aws_vpc.admin-default-sa-east-1.id
}

resource "aws_subnet" "admin-default-public-sa-east-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1c"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-sa-east-1.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-sa-east-1.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "sa-east-1c"
    Connectivity     = "public"
    Environment      = "admin"
    Name             = "admin-default-public-sa-east-1c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.admin-default-sa-east-1.id
}

resource "aws_vpc" "admin-default-sa-east-1" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "192.168.40.0/21"
  enable_dns_hostnames             = true
  enable_dns_support               = true
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
}

resource "aws_vpc_endpoint" "admin-default-sa-east-1" {
  route_table_ids = [aws_vpc.admin-default-sa-east-1.default_route_table_id]
  service_name    = "com.amazonaws.sa-east-1.s3"
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.admin-default-sa-east-1.id
}

data "aws_organizations_organization" "current" {
}
