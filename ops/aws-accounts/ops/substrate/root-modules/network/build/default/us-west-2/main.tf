# managed by Substrate; do not edit by hand

resource "aws_egress_only_internet_gateway" "build-default-us-west-2" {
  tags = {
    Environment = "build"
    Name        = "build-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_eip" "build-default-us-west-2a" {
  depends_on = [aws_internet_gateway.build-default-us-west-2]
  tags = {
    AvailabilityZone = "us-west-2a"
    Environment      = "build"
    Name             = "build-default-nat-gateway-us-west-2a"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_eip" "build-default-us-west-2c" {
  depends_on = [aws_internet_gateway.build-default-us-west-2]
  tags = {
    AvailabilityZone = "us-west-2c"
    Environment      = "build"
    Name             = "build-default-nat-gateway-us-west-2c"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_eip" "build-default-us-west-2d" {
  depends_on = [aws_internet_gateway.build-default-us-west-2]
  tags = {
    AvailabilityZone = "us-west-2d"
    Environment      = "build"
    Name             = "build-default-nat-gateway-us-west-2d"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_internet_gateway" "build-default-us-west-2" {
  tags = {
    Environment = "build"
    Name        = "build-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_nat_gateway" "build-default-us-west-2a" {
  allocation_id = aws_eip.build-default-us-west-2a.id
  subnet_id     = aws_subnet.build-default-public-us-west-2a.id
  tags = {
    AvailabilityZone = "us-west-2a"
    Environment      = "build"
    Name             = "build-default-us-west-2a"
    Quality          = "default"
  }
}

resource "aws_nat_gateway" "build-default-us-west-2c" {
  allocation_id = aws_eip.build-default-us-west-2c.id
  subnet_id     = aws_subnet.build-default-public-us-west-2c.id
  tags = {
    AvailabilityZone = "us-west-2c"
    Environment      = "build"
    Name             = "build-default-us-west-2c"
    Quality          = "default"
  }
}

resource "aws_nat_gateway" "build-default-us-west-2d" {
  allocation_id = aws_eip.build-default-us-west-2d.id
  subnet_id     = aws_subnet.build-default-public-us-west-2d.id
  tags = {
    AvailabilityZone = "us-west-2d"
    Environment      = "build"
    Name             = "build-default-us-west-2d"
    Quality          = "default"
  }
}

resource "aws_route" "build-default-private-us-west-2a-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.build-default-us-west-2.id
  route_table_id              = aws_route_table.build-default-private-us-west-2a.id
}

resource "aws_route" "build-default-private-us-west-2c-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.build-default-us-west-2.id
  route_table_id              = aws_route_table.build-default-private-us-west-2c.id
}

resource "aws_route" "build-default-private-us-west-2d-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.build-default-us-west-2.id
  route_table_id              = aws_route_table.build-default-private-us-west-2d.id
}

resource "aws_route" "build-default-public-internet-ipv4-us-west-2" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.build-default-us-west-2.id
  route_table_id         = aws_vpc.build-default-us-west-2.default_route_table_id
}

resource "aws_route" "build-default-public-internet-ipv6-us-west-2" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.build-default-us-west-2.id
  route_table_id              = aws_vpc.build-default-us-west-2.default_route_table_id
}

resource "aws_route" "build-default-us-west-2a" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.build-default-us-west-2a.id
  route_table_id         = aws_route_table.build-default-private-us-west-2a.id
}

resource "aws_route" "build-default-us-west-2c" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.build-default-us-west-2c.id
  route_table_id         = aws_route_table.build-default-private-us-west-2c.id
}

resource "aws_route" "build-default-us-west-2d" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.build-default-us-west-2d.id
  route_table_id         = aws_route_table.build-default-private-us-west-2d.id
}

resource "aws_route_table" "build-default-private-us-west-2a" {
  tags = {
    AvailabilityZone = "us-west-2a"
    Connectivity     = "private"
    Environment      = "build"
    Name             = "build-default-private-us-west-2a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_route_table" "build-default-private-us-west-2c" {
  tags = {
    AvailabilityZone = "us-west-2c"
    Connectivity     = "private"
    Environment      = "build"
    Name             = "build-default-private-us-west-2c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_route_table" "build-default-private-us-west-2d" {
  tags = {
    AvailabilityZone = "us-west-2d"
    Connectivity     = "private"
    Environment      = "build"
    Name             = "build-default-private-us-west-2d"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_route_table_association" "build-default-private-us-west-2a" {
  route_table_id = aws_route_table.build-default-private-us-west-2a.id
  subnet_id      = aws_subnet.build-default-private-us-west-2a.id
}

resource "aws_route_table_association" "build-default-private-us-west-2c" {
  route_table_id = aws_route_table.build-default-private-us-west-2c.id
  subnet_id      = aws_subnet.build-default-private-us-west-2c.id
}

resource "aws_route_table_association" "build-default-private-us-west-2d" {
  route_table_id = aws_route_table.build-default-private-us-west-2d.id
  subnet_id      = aws_subnet.build-default-private-us-west-2d.id
}

resource "aws_route_table_association" "build-default-public-us-west-2a" {
  route_table_id = aws_vpc.build-default-us-west-2.default_route_table_id
  subnet_id      = aws_subnet.build-default-public-us-west-2a.id
}

resource "aws_route_table_association" "build-default-public-us-west-2c" {
  route_table_id = aws_vpc.build-default-us-west-2.default_route_table_id
  subnet_id      = aws_subnet.build-default-public-us-west-2c.id
}

resource "aws_route_table_association" "build-default-public-us-west-2d" {
  route_table_id = aws_vpc.build-default-us-west-2.default_route_table_id
  subnet_id      = aws_subnet.build-default-public-us-west-2d.id
}

resource "aws_subnet" "build-default-private-us-west-2a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-west-2a"
  cidr_block                      = cidrsubnet(aws_vpc.build-default-us-west-2.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.build-default-us-west-2.ipv6_cidr_block, 8, 129)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "us-west-2a"
    Connectivity     = "private"
    Environment      = "build"
    Name             = "build-default-private-us-west-2a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_subnet" "build-default-private-us-west-2c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-west-2c"
  cidr_block                      = cidrsubnet(aws_vpc.build-default-us-west-2.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.build-default-us-west-2.ipv6_cidr_block, 8, 130)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "us-west-2c"
    Connectivity     = "private"
    Environment      = "build"
    Name             = "build-default-private-us-west-2c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_subnet" "build-default-private-us-west-2d" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-west-2d"
  cidr_block                      = cidrsubnet(aws_vpc.build-default-us-west-2.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.build-default-us-west-2.ipv6_cidr_block, 8, 131)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "us-west-2d"
    Connectivity     = "private"
    Environment      = "build"
    Name             = "build-default-private-us-west-2d"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_subnet" "build-default-public-us-west-2a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-west-2a"
  cidr_block                      = cidrsubnet(aws_vpc.build-default-us-west-2.cidr_block, 4, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.build-default-us-west-2.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-west-2a"
    Connectivity     = "public"
    Environment      = "build"
    Name             = "build-default-public-us-west-2a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_subnet" "build-default-public-us-west-2c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-west-2c"
  cidr_block                      = cidrsubnet(aws_vpc.build-default-us-west-2.cidr_block, 4, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.build-default-us-west-2.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-west-2c"
    Connectivity     = "public"
    Environment      = "build"
    Name             = "build-default-public-us-west-2c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_subnet" "build-default-public-us-west-2d" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-west-2d"
  cidr_block                      = cidrsubnet(aws_vpc.build-default-us-west-2.cidr_block, 4, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.build-default-us-west-2.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-west-2d"
    Connectivity     = "public"
    Environment      = "build"
    Name             = "build-default-public-us-west-2d"
    Quality          = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

resource "aws_vpc" "build-default-us-west-2" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "10.3.192.0/18"
  enable_dns_hostnames             = true
  enable_dns_support               = true
  tags = {
    Environment = "build"
    Name        = "build-default"
    Quality     = "default"
  }
}

resource "aws_vpc_endpoint" "build-default-us-west-2" {
  route_table_ids = [
    aws_vpc.build-default-us-west-2.default_route_table_id,
    aws_route_table.build-default-private-us-west-2a.id,
    aws_route_table.build-default-private-us-west-2c.id,
    aws_route_table.build-default-private-us-west-2d.id,
  ]
  service_name = "com.amazonaws.us-west-2.s3"
  tags = {
    Environment = "build"
    Name        = "build-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.build-default-us-west-2.id
}

data "aws_organizations_organization" "current" {
}
