# managed by Substrate; do not edit by hand

resource "aws_egress_only_internet_gateway" "sandbox-default-us-east-2" {
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_eip" "sandbox-default-us-east-2a" {
  depends_on = [aws_internet_gateway.sandbox-default-us-east-2]
  tags = {
    AvailabilityZone = "us-east-2a"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-nat-gateway-us-east-2a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-us-east-2b" {
  depends_on = [aws_internet_gateway.sandbox-default-us-east-2]
  tags = {
    AvailabilityZone = "us-east-2b"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-nat-gateway-us-east-2b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-us-east-2c" {
  depends_on = [aws_internet_gateway.sandbox-default-us-east-2]
  tags = {
    AvailabilityZone = "us-east-2c"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-nat-gateway-us-east-2c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc = true
}

resource "aws_internet_gateway" "sandbox-default-us-east-2" {
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_nat_gateway" "sandbox-default-us-east-2a" {
  allocation_id = aws_eip.sandbox-default-us-east-2a.id
  subnet_id     = aws_subnet.sandbox-default-public-us-east-2a.id
  tags = {
    AvailabilityZone = "us-east-2a"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-us-east-2a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_nat_gateway" "sandbox-default-us-east-2b" {
  allocation_id = aws_eip.sandbox-default-us-east-2b.id
  subnet_id     = aws_subnet.sandbox-default-public-us-east-2b.id
  tags = {
    AvailabilityZone = "us-east-2b"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-us-east-2b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_nat_gateway" "sandbox-default-us-east-2c" {
  allocation_id = aws_eip.sandbox-default-us-east-2c.id
  subnet_id     = aws_subnet.sandbox-default-public-us-east-2c.id
  tags = {
    AvailabilityZone = "us-east-2c"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-us-east-2c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_ram_resource_share" "sandbox-default-us-east-2" {
  allow_external_principals = false
  name                      = "sandbox-default-us-east-2"
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_ram_resource_association" "sandbox-default-private-us-east-2a" {
  resource_arn       = aws_subnet.sandbox-default-private-us-east-2a.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-us-east-2.arn
}

resource "aws_ram_resource_association" "sandbox-default-private-us-east-2b" {
  resource_arn       = aws_subnet.sandbox-default-private-us-east-2b.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-us-east-2.arn
}

resource "aws_ram_resource_association" "sandbox-default-private-us-east-2c" {
  resource_arn       = aws_subnet.sandbox-default-private-us-east-2c.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-us-east-2.arn
}

resource "aws_ram_resource_association" "sandbox-default-public-us-east-2a" {
  resource_arn       = aws_subnet.sandbox-default-public-us-east-2a.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-us-east-2.arn
}

resource "aws_ram_resource_association" "sandbox-default-public-us-east-2b" {
  resource_arn       = aws_subnet.sandbox-default-public-us-east-2b.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-us-east-2.arn
}

resource "aws_ram_resource_association" "sandbox-default-public-us-east-2c" {
  resource_arn       = aws_subnet.sandbox-default-public-us-east-2c.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-us-east-2.arn
}

resource "aws_ram_principal_association" "sandbox-default-us-east-2" {
  principal          = data.aws_organizations_organization.current.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-us-east-2.arn
}

resource "aws_route" "sandbox-default-private-us-east-2a-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-us-east-2.id
  route_table_id              = aws_route_table.sandbox-default-private-us-east-2a.id
}

resource "aws_route" "sandbox-default-private-us-east-2b-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-us-east-2.id
  route_table_id              = aws_route_table.sandbox-default-private-us-east-2b.id
}

resource "aws_route" "sandbox-default-private-us-east-2c-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-us-east-2.id
  route_table_id              = aws_route_table.sandbox-default-private-us-east-2c.id
}

resource "aws_route" "sandbox-default-public-internet-ipv4-us-east-2" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.sandbox-default-us-east-2.id
  route_table_id         = aws_vpc.sandbox-default-us-east-2.default_route_table_id
}

resource "aws_route" "sandbox-default-public-internet-ipv6-us-east-2" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.sandbox-default-us-east-2.id
  route_table_id              = aws_vpc.sandbox-default-us-east-2.default_route_table_id
}

resource "aws_route" "sandbox-default-us-east-2a" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-us-east-2a.id
  route_table_id         = aws_route_table.sandbox-default-private-us-east-2a.id
}

resource "aws_route" "sandbox-default-us-east-2b" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-us-east-2b.id
  route_table_id         = aws_route_table.sandbox-default-private-us-east-2b.id
}

resource "aws_route" "sandbox-default-us-east-2c" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-us-east-2c.id
  route_table_id         = aws_route_table.sandbox-default-private-us-east-2c.id
}

resource "aws_route_table" "sandbox-default-private-us-east-2a" {
  tags = {
    AvailabilityZone = "us-east-2a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-us-east-2a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_route_table" "sandbox-default-private-us-east-2b" {
  tags = {
    AvailabilityZone = "us-east-2b"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-us-east-2b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_route_table" "sandbox-default-private-us-east-2c" {
  tags = {
    AvailabilityZone = "us-east-2c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-us-east-2c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_route_table_association" "sandbox-default-private-us-east-2a" {
  route_table_id = aws_route_table.sandbox-default-private-us-east-2a.id
  subnet_id      = aws_subnet.sandbox-default-private-us-east-2a.id
}

resource "aws_route_table_association" "sandbox-default-private-us-east-2b" {
  route_table_id = aws_route_table.sandbox-default-private-us-east-2b.id
  subnet_id      = aws_subnet.sandbox-default-private-us-east-2b.id
}

resource "aws_route_table_association" "sandbox-default-private-us-east-2c" {
  route_table_id = aws_route_table.sandbox-default-private-us-east-2c.id
  subnet_id      = aws_subnet.sandbox-default-private-us-east-2c.id
}

resource "aws_route_table_association" "sandbox-default-public-us-east-2a" {
  route_table_id = aws_vpc.sandbox-default-us-east-2.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-us-east-2a.id
}

resource "aws_route_table_association" "sandbox-default-public-us-east-2b" {
  route_table_id = aws_vpc.sandbox-default-us-east-2.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-us-east-2b.id
}

resource "aws_route_table_association" "sandbox-default-public-us-east-2c" {
  route_table_id = aws_vpc.sandbox-default-us-east-2.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-us-east-2c.id
}

resource "aws_subnet" "sandbox-default-private-us-east-2a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-us-east-2.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-us-east-2.ipv6_cidr_block, 8, 129)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "us-east-2a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-us-east-2a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_subnet" "sandbox-default-private-us-east-2b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2b"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-us-east-2.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-us-east-2.ipv6_cidr_block, 8, 130)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "us-east-2b"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-us-east-2b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_subnet" "sandbox-default-private-us-east-2c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-us-east-2.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-us-east-2.ipv6_cidr_block, 8, 131)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "us-east-2c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-us-east-2c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_subnet" "sandbox-default-public-us-east-2a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-us-east-2.cidr_block, 4, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-us-east-2.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-east-2a"
    Connectivity     = "public"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-public-us-east-2a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_subnet" "sandbox-default-public-us-east-2b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2b"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-us-east-2.cidr_block, 4, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-us-east-2.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-east-2b"
    Connectivity     = "public"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-public-us-east-2b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_subnet" "sandbox-default-public-us-east-2c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "us-east-2c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-us-east-2.cidr_block, 4, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-us-east-2.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "us-east-2c"
    Connectivity     = "public"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-public-us-east-2c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

resource "aws_vpc" "sandbox-default-us-east-2" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "10.0.0.0/18"
  enable_dns_hostnames             = true
  enable_dns_support               = true
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_vpc_endpoint" "sandbox-default-us-east-2" {
  route_table_ids = [
    aws_vpc.sandbox-default-us-east-2.default_route_table_id,
    aws_route_table.sandbox-default-private-us-east-2a.id,
    aws_route_table.sandbox-default-private-us-east-2b.id,
    aws_route_table.sandbox-default-private-us-east-2c.id,
  ]
  service_name = "com.amazonaws.us-east-2.s3"
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-us-east-2.id
}

data "aws_organizations_organization" "current" {
}
