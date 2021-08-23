# managed by Substrate; do not edit by hand

resource "aws_internet_gateway" "admin-default-us-east-2" {
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.admin-default-us-east-2.id
}

resource "aws_route" "admin-default-public-internet-ipv4-us-east-2" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.admin-default-us-east-2.id
  route_table_id         = aws_vpc.admin-default-us-east-2.default_route_table_id
}

resource "aws_route" "admin-default-public-internet-ipv6-us-east-2" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.admin-default-us-east-2.id
  route_table_id              = aws_vpc.admin-default-us-east-2.default_route_table_id
}

resource "aws_route_table_association" "admin-default-public-us-east-2a" {
  route_table_id = aws_vpc.admin-default-us-east-2.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-us-east-2a.id
}

resource "aws_route_table_association" "admin-default-public-us-east-2b" {
  route_table_id = aws_vpc.admin-default-us-east-2.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-us-east-2b.id
}

resource "aws_route_table_association" "admin-default-public-us-east-2c" {
  route_table_id = aws_vpc.admin-default-us-east-2.default_route_table_id
  subnet_id      = aws_subnet.admin-default-public-us-east-2c.id
}

resource "aws_subnet" "admin-default-public-us-east-2a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2a"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-us-east-2.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-us-east-2.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-east-2a"
    Connectivity     = "public"
    Environment      = "admin"
    Name             = "admin-default-public-us-east-2a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.admin-default-us-east-2.id
}

resource "aws_subnet" "admin-default-public-us-east-2b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2b"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-us-east-2.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-us-east-2.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-east-2b"
    Connectivity     = "public"
    Environment      = "admin"
    Name             = "admin-default-public-us-east-2b"
    Quality          = "default"
  }
  vpc_id = aws_vpc.admin-default-us-east-2.id
}

resource "aws_subnet" "admin-default-public-us-east-2c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2c"
  cidr_block                      = cidrsubnet(aws_vpc.admin-default-us-east-2.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.admin-default-us-east-2.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-east-2c"
    Connectivity     = "public"
    Environment      = "admin"
    Name             = "admin-default-public-us-east-2c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.admin-default-us-east-2.id
}

resource "aws_vpc" "admin-default-us-east-2" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "192.168.0.0/21"
  enable_dns_hostnames             = true
  enable_dns_support               = true
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
}

resource "aws_vpc_endpoint" "admin-default-us-east-2" {
  route_table_ids = [aws_vpc.admin-default-us-east-2.default_route_table_id]
  service_name    = "com.amazonaws.us-east-2.s3"
  tags = {
    Environment = "admin"
    Name        = "admin-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.admin-default-us-east-2.id
}

data "aws_organizations_organization" "current" {
}
