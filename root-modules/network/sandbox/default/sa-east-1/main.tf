# managed by Substrate; do not edit by hand

resource "aws_egress_only_internet_gateway" "sandbox-default-sa-east-1" {
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_eip" "sandbox-default-sa-east-1a" {
  depends_on = [aws_internet_gateway.sandbox-default-sa-east-1]
  tags = {
    AvailabilityZone = "sa-east-1a"
    Environment      = "sandbox"
    Name             = "sandbox-default-nat-gateway-sa-east-1a"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-sa-east-1b" {
  depends_on = [aws_internet_gateway.sandbox-default-sa-east-1]
  tags = {
    AvailabilityZone = "sa-east-1b"
    Environment      = "sandbox"
    Name             = "sandbox-default-nat-gateway-sa-east-1b"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-sa-east-1c" {
  depends_on = [aws_internet_gateway.sandbox-default-sa-east-1]
  tags = {
    AvailabilityZone = "sa-east-1c"
    Environment      = "sandbox"
    Name             = "sandbox-default-nat-gateway-sa-east-1c"
    Quality          = "default"
  }
  vpc = true
}

resource "aws_internet_gateway" "sandbox-default-sa-east-1" {
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_nat_gateway" "sandbox-default-sa-east-1a" {
  allocation_id = aws_eip.sandbox-default-sa-east-1a.id
  subnet_id     = aws_subnet.sandbox-default-public-sa-east-1a.id
  tags = {
    AvailabilityZone = "sa-east-1a"
    Environment      = "sandbox"
    Name             = "sandbox-default-sa-east-1a"
    Quality          = "default"
  }
}

resource "aws_nat_gateway" "sandbox-default-sa-east-1b" {
  allocation_id = aws_eip.sandbox-default-sa-east-1b.id
  subnet_id     = aws_subnet.sandbox-default-public-sa-east-1b.id
  tags = {
    AvailabilityZone = "sa-east-1b"
    Environment      = "sandbox"
    Name             = "sandbox-default-sa-east-1b"
    Quality          = "default"
  }
}

resource "aws_nat_gateway" "sandbox-default-sa-east-1c" {
  allocation_id = aws_eip.sandbox-default-sa-east-1c.id
  subnet_id     = aws_subnet.sandbox-default-public-sa-east-1c.id
  tags = {
    AvailabilityZone = "sa-east-1c"
    Environment      = "sandbox"
    Name             = "sandbox-default-sa-east-1c"
    Quality          = "default"
  }
}

resource "aws_route" "sandbox-default-private-sa-east-1a-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-sa-east-1.id
  route_table_id              = aws_route_table.sandbox-default-private-sa-east-1a.id
}

resource "aws_route" "sandbox-default-private-sa-east-1b-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-sa-east-1.id
  route_table_id              = aws_route_table.sandbox-default-private-sa-east-1b.id
}

resource "aws_route" "sandbox-default-private-sa-east-1c-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-sa-east-1.id
  route_table_id              = aws_route_table.sandbox-default-private-sa-east-1c.id
}

resource "aws_route" "sandbox-default-public-internet-ipv4-sa-east-1" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.sandbox-default-sa-east-1.id
  route_table_id         = aws_vpc.sandbox-default-sa-east-1.default_route_table_id
}

resource "aws_route" "sandbox-default-public-internet-ipv6-sa-east-1" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.sandbox-default-sa-east-1.id
  route_table_id              = aws_vpc.sandbox-default-sa-east-1.default_route_table_id
}

resource "aws_route" "sandbox-default-sa-east-1a" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-sa-east-1a.id
  route_table_id         = aws_route_table.sandbox-default-private-sa-east-1a.id
}

resource "aws_route" "sandbox-default-sa-east-1b" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-sa-east-1b.id
  route_table_id         = aws_route_table.sandbox-default-private-sa-east-1b.id
}

resource "aws_route" "sandbox-default-sa-east-1c" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-sa-east-1c.id
  route_table_id         = aws_route_table.sandbox-default-private-sa-east-1c.id
}

resource "aws_route_table" "sandbox-default-private-sa-east-1a" {
  tags = {
    AvailabilityZone = "sa-east-1a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-sa-east-1a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_route_table" "sandbox-default-private-sa-east-1b" {
  tags = {
    AvailabilityZone = "sa-east-1b"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-sa-east-1b"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_route_table" "sandbox-default-private-sa-east-1c" {
  tags = {
    AvailabilityZone = "sa-east-1c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-sa-east-1c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_route_table_association" "sandbox-default-private-sa-east-1a" {
  route_table_id = aws_route_table.sandbox-default-private-sa-east-1a.id
  subnet_id      = aws_subnet.sandbox-default-private-sa-east-1a.id
}

resource "aws_route_table_association" "sandbox-default-private-sa-east-1b" {
  route_table_id = aws_route_table.sandbox-default-private-sa-east-1b.id
  subnet_id      = aws_subnet.sandbox-default-private-sa-east-1b.id
}

resource "aws_route_table_association" "sandbox-default-private-sa-east-1c" {
  route_table_id = aws_route_table.sandbox-default-private-sa-east-1c.id
  subnet_id      = aws_subnet.sandbox-default-private-sa-east-1c.id
}

resource "aws_route_table_association" "sandbox-default-public-sa-east-1a" {
  route_table_id = aws_vpc.sandbox-default-sa-east-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-sa-east-1a.id
}

resource "aws_route_table_association" "sandbox-default-public-sa-east-1b" {
  route_table_id = aws_vpc.sandbox-default-sa-east-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-sa-east-1b.id
}

resource "aws_route_table_association" "sandbox-default-public-sa-east-1c" {
  route_table_id = aws_vpc.sandbox-default-sa-east-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-sa-east-1c.id
}

resource "aws_subnet" "sandbox-default-private-sa-east-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.ipv6_cidr_block, 8, 129)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "sa-east-1a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-sa-east-1a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_subnet" "sandbox-default-private-sa-east-1b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1b"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.ipv6_cidr_block, 8, 130)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "sa-east-1b"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-sa-east-1b"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_subnet" "sandbox-default-private-sa-east-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.ipv6_cidr_block, 8, 131)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "sa-east-1c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Name             = "sandbox-default-private-sa-east-1c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_subnet" "sandbox-default-public-sa-east-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.cidr_block, 4, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "sa-east-1a"
    Connectivity     = "public"
    Environment      = "sandbox"
    Name             = "sandbox-default-public-sa-east-1a"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_subnet" "sandbox-default-public-sa-east-1b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1b"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.cidr_block, 4, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "sa-east-1b"
    Connectivity     = "public"
    Environment      = "sandbox"
    Name             = "sandbox-default-public-sa-east-1b"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_subnet" "sandbox-default-public-sa-east-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "sa-east-1c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.cidr_block, 4, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-sa-east-1.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "sa-east-1c"
    Connectivity     = "public"
    Environment      = "sandbox"
    Name             = "sandbox-default-public-sa-east-1c"
    Quality          = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

resource "aws_vpc" "sandbox-default-sa-east-1" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "10.2.128.0/18"
  enable_dns_hostnames             = true
  enable_dns_support               = true
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
}

resource "aws_vpc_endpoint" "sandbox-default-sa-east-1" {
  route_table_ids = [
    aws_vpc.sandbox-default-sa-east-1.default_route_table_id,
    aws_route_table.sandbox-default-private-sa-east-1a.id,
    aws_route_table.sandbox-default-private-sa-east-1b.id,
    aws_route_table.sandbox-default-private-sa-east-1c.id,
  ]
  service_name = "com.amazonaws.sa-east-1.s3"
  tags = {
    Environment = "sandbox"
    Name        = "sandbox-default"
    Quality     = "default"
  }
  vpc_id = aws_vpc.sandbox-default-sa-east-1.id
}

data "aws_organizations_organization" "current" {
}
