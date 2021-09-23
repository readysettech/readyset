# managed by Substrate; do not edit by hand

resource "aws_egress_only_internet_gateway" "sandbox-default-ap-northeast-1" {
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_eip" "sandbox-default-ap-northeast-1a" {
  depends_on = [aws_internet_gateway.sandbox-default-ap-northeast-1]
  tags = {
    AvailabilityZone = "ap-northeast-1a"
    Environment      = "sandbox"
    Name             = "sandbox-default-nat-gateway-ap-northeast-1a"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-ap-northeast-1c" {
  depends_on = [aws_internet_gateway.sandbox-default-ap-northeast-1]
  tags = {
    AvailabilityZone = "ap-northeast-1c"
    Environment      = "sandbox"
    Name             = "sandbox-default-nat-gateway-ap-northeast-1c"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-ap-northeast-1d" {
  depends_on = [aws_internet_gateway.sandbox-default-ap-northeast-1]
  tags = {
    AvailabilityZone = "ap-northeast-1d"
    Environment      = "sandbox"
    Name             = "sandbox-default-nat-gateway-ap-northeast-1d"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_internet_gateway" "sandbox-default-ap-northeast-1" {
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_nat_gateway" "sandbox-default-ap-northeast-1a" {
  allocation_id = aws_eip.sandbox-default-ap-northeast-1a.id
  subnet_id     = aws_subnet.sandbox-default-public-ap-northeast-1a.id
  tags = {
    AvailabilityZone = "ap-northeast-1a"
    Environment      = "sandbox"
    Name             = "sandbox-default-ap-northeast-1a"
    Quality          = "default"
  }
}

resource "aws_nat_gateway" "sandbox-default-ap-northeast-1c" {
  allocation_id = aws_eip.sandbox-default-ap-northeast-1c.id
  subnet_id     = aws_subnet.sandbox-default-public-ap-northeast-1c.id
  tags = {
    AvailabilityZone = "ap-northeast-1c"
    Environment      = "sandbox"
    Name             = "sandbox-default-ap-northeast-1c"
    Quality          = "default"
  }
}

resource "aws_nat_gateway" "sandbox-default-ap-northeast-1d" {
  allocation_id = aws_eip.sandbox-default-ap-northeast-1d.id
  subnet_id     = aws_subnet.sandbox-default-public-ap-northeast-1d.id
  tags = {
    AvailabilityZone = "ap-northeast-1d"
    Environment      = "sandbox"
    Name             = "sandbox-default-ap-northeast-1d"
    Quality          = "default"
  }
}

resource "aws_route" "sandbox-default-ap-northeast-1a" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-ap-northeast-1a.id
  route_table_id         = aws_route_table.sandbox-default-private-ap-northeast-1a.id
}

resource "aws_route" "sandbox-default-ap-northeast-1c" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-ap-northeast-1c.id
  route_table_id         = aws_route_table.sandbox-default-private-ap-northeast-1c.id
}

resource "aws_route" "sandbox-default-ap-northeast-1d" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-ap-northeast-1d.id
  route_table_id         = aws_route_table.sandbox-default-private-ap-northeast-1d.id
}

resource "aws_route" "sandbox-default-private-ap-northeast-1a-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-ap-northeast-1.id
  route_table_id              = aws_route_table.sandbox-default-private-ap-northeast-1a.id
}

resource "aws_route" "sandbox-default-private-ap-northeast-1c-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-ap-northeast-1.id
  route_table_id              = aws_route_table.sandbox-default-private-ap-northeast-1c.id
}

resource "aws_route" "sandbox-default-private-ap-northeast-1d-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-ap-northeast-1.id
  route_table_id              = aws_route_table.sandbox-default-private-ap-northeast-1d.id
}

resource "aws_route" "sandbox-default-public-internet-ipv4-ap-northeast-1" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.sandbox-default-ap-northeast-1.id
  route_table_id         = aws_vpc.sandbox-default-ap-northeast-1.default_route_table_id
}

resource "aws_route" "sandbox-default-public-internet-ipv6-ap-northeast-1" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.sandbox-default-ap-northeast-1.id
  route_table_id              = aws_vpc.sandbox-default-ap-northeast-1.default_route_table_id
}

resource "aws_route_table" "sandbox-default-private-ap-northeast-1a" {
  tags = {
    AvailabilityZone = "ap-northeast-1a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-ap-northeast-1a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_route_table" "sandbox-default-private-ap-northeast-1c" {
  tags = {
    AvailabilityZone = "ap-northeast-1c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-ap-northeast-1c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_route_table" "sandbox-default-private-ap-northeast-1d" {
  tags = {
    AvailabilityZone = "ap-northeast-1d"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-ap-northeast-1d"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_route_table_association" "sandbox-default-private-ap-northeast-1a" {
  route_table_id = aws_route_table.sandbox-default-private-ap-northeast-1a.id
  subnet_id      = aws_subnet.sandbox-default-private-ap-northeast-1a.id
}

resource "aws_route_table_association" "sandbox-default-private-ap-northeast-1c" {
  route_table_id = aws_route_table.sandbox-default-private-ap-northeast-1c.id
  subnet_id      = aws_subnet.sandbox-default-private-ap-northeast-1c.id
}

resource "aws_route_table_association" "sandbox-default-private-ap-northeast-1d" {
  route_table_id = aws_route_table.sandbox-default-private-ap-northeast-1d.id
  subnet_id      = aws_subnet.sandbox-default-private-ap-northeast-1d.id
}

resource "aws_route_table_association" "sandbox-default-public-ap-northeast-1a" {
  route_table_id = aws_vpc.sandbox-default-ap-northeast-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-ap-northeast-1a.id
}

resource "aws_route_table_association" "sandbox-default-public-ap-northeast-1c" {
  route_table_id = aws_vpc.sandbox-default-ap-northeast-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-ap-northeast-1c.id
}

resource "aws_route_table_association" "sandbox-default-public-ap-northeast-1d" {
  route_table_id = aws_vpc.sandbox-default-ap-northeast-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-ap-northeast-1d.id
}

resource "aws_subnet" "sandbox-default-private-ap-northeast-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "ap-northeast-1a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.ipv6_cidr_block, 8, 129)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "ap-northeast-1a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-ap-northeast-1a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_subnet" "sandbox-default-private-ap-northeast-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "ap-northeast-1c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.ipv6_cidr_block, 8, 130)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "ap-northeast-1c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-ap-northeast-1c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_subnet" "sandbox-default-private-ap-northeast-1d" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "ap-northeast-1d"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.ipv6_cidr_block, 8, 131)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "ap-northeast-1d"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-ap-northeast-1d"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_subnet" "sandbox-default-public-ap-northeast-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "ap-northeast-1a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.cidr_block, 4, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "ap-northeast-1a"
    Connectivity     = "public"
    Environment      = "sandbox"
    Name             = "sandbox-default-public-ap-northeast-1a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_subnet" "sandbox-default-public-ap-northeast-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "ap-northeast-1c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.cidr_block, 4, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "ap-northeast-1c"
    Connectivity     = "public"
    Environment      = "sandbox"
    Name             = "sandbox-default-public-ap-northeast-1c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_subnet" "sandbox-default-public-ap-northeast-1d" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "ap-northeast-1d"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.cidr_block, 4, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-ap-northeast-1.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "ap-northeast-1d"
    Connectivity     = "public"
    Environment      = "sandbox"
    Name             = "sandbox-default-public-ap-northeast-1d"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

resource "aws_vpc" "sandbox-default-ap-northeast-1" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "10.2.0.0/18"
  enable_dns_hostnames             = true
  enable_dns_support               = true
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
}

resource "aws_vpc_endpoint" "sandbox-default-ap-northeast-1" {
  route_table_ids = [
    aws_vpc.sandbox-default-ap-northeast-1.default_route_table_id,
    aws_route_table.sandbox-default-private-ap-northeast-1a.id,
    aws_route_table.sandbox-default-private-ap-northeast-1c.id,
    aws_route_table.sandbox-default-private-ap-northeast-1d.id,
  ]
  service_name = "com.amazonaws.ap-northeast-1.s3"
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.sandbox-default-ap-northeast-1.id
}

data "aws_organizations_organization" "current" {
}
