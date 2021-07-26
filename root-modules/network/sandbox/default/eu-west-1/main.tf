# managed by Substrate; do not edit by hand

resource "aws_egress_only_internet_gateway" "sandbox-default-eu-west-1" {
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_eip" "sandbox-default-eu-west-1a" {
  depends_on = [aws_internet_gateway.sandbox-default-eu-west-1]
  tags = {
    AvailabilityZone = "eu-west-1a"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-nat-gateway-eu-west-1a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-eu-west-1b" {
  depends_on = [aws_internet_gateway.sandbox-default-eu-west-1]
  tags = {
    AvailabilityZone = "eu-west-1b"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-nat-gateway-eu-west-1b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc = true
}

resource "aws_eip" "sandbox-default-eu-west-1c" {
  depends_on = [aws_internet_gateway.sandbox-default-eu-west-1]
  tags = {
    AvailabilityZone = "eu-west-1c"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-nat-gateway-eu-west-1c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc = true
}

resource "aws_internet_gateway" "sandbox-default-eu-west-1" {
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_nat_gateway" "sandbox-default-eu-west-1a" {
  allocation_id = aws_eip.sandbox-default-eu-west-1a.id
  subnet_id     = aws_subnet.sandbox-default-public-eu-west-1a.id
  tags = {
    AvailabilityZone = "eu-west-1a"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-eu-west-1a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_nat_gateway" "sandbox-default-eu-west-1b" {
  allocation_id = aws_eip.sandbox-default-eu-west-1b.id
  subnet_id     = aws_subnet.sandbox-default-public-eu-west-1b.id
  tags = {
    AvailabilityZone = "eu-west-1b"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-eu-west-1b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_nat_gateway" "sandbox-default-eu-west-1c" {
  allocation_id = aws_eip.sandbox-default-eu-west-1c.id
  subnet_id     = aws_subnet.sandbox-default-public-eu-west-1c.id
  tags = {
    AvailabilityZone = "eu-west-1c"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-eu-west-1c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_ram_resource_share" "sandbox-default-eu-west-1" {
  allow_external_principals = false
  name                      = "sandbox-default-eu-west-1"
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
}

resource "aws_ram_principal_association" "sandbox-default-eu-west-1" {
  principal          = data.aws_organizations_organization.current.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "sandbox-default-private-eu-west-1a" {
  resource_arn       = aws_subnet.sandbox-default-private-eu-west-1a.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "sandbox-default-private-eu-west-1b" {
  resource_arn       = aws_subnet.sandbox-default-private-eu-west-1b.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "sandbox-default-private-eu-west-1c" {
  resource_arn       = aws_subnet.sandbox-default-private-eu-west-1c.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "sandbox-default-public-eu-west-1a" {
  resource_arn       = aws_subnet.sandbox-default-public-eu-west-1a.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "sandbox-default-public-eu-west-1b" {
  resource_arn       = aws_subnet.sandbox-default-public-eu-west-1b.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-eu-west-1.arn
}

resource "aws_ram_resource_association" "sandbox-default-public-eu-west-1c" {
  resource_arn       = aws_subnet.sandbox-default-public-eu-west-1c.arn
  resource_share_arn = aws_ram_resource_share.sandbox-default-eu-west-1.arn
}

resource "aws_route" "sandbox-default-eu-west-1a" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-eu-west-1a.id
  route_table_id         = aws_route_table.sandbox-default-private-eu-west-1a.id
}

resource "aws_route" "sandbox-default-eu-west-1b" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-eu-west-1b.id
  route_table_id         = aws_route_table.sandbox-default-private-eu-west-1b.id
}

resource "aws_route" "sandbox-default-eu-west-1c" {
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.sandbox-default-eu-west-1c.id
  route_table_id         = aws_route_table.sandbox-default-private-eu-west-1c.id
}

resource "aws_route" "sandbox-default-private-eu-west-1a-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-eu-west-1.id
  route_table_id              = aws_route_table.sandbox-default-private-eu-west-1a.id
}

resource "aws_route" "sandbox-default-private-eu-west-1b-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-eu-west-1.id
  route_table_id              = aws_route_table.sandbox-default-private-eu-west-1b.id
}

resource "aws_route" "sandbox-default-private-eu-west-1c-private-internet-ipv6" {
  destination_ipv6_cidr_block = "::/0"
  egress_only_gateway_id      = aws_egress_only_internet_gateway.sandbox-default-eu-west-1.id
  route_table_id              = aws_route_table.sandbox-default-private-eu-west-1c.id
}

resource "aws_route" "sandbox-default-public-internet-ipv4-eu-west-1" {
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.sandbox-default-eu-west-1.id
  route_table_id         = aws_vpc.sandbox-default-eu-west-1.default_route_table_id
}

resource "aws_route" "sandbox-default-public-internet-ipv6-eu-west-1" {
  destination_ipv6_cidr_block = "::/0"
  gateway_id                  = aws_internet_gateway.sandbox-default-eu-west-1.id
  route_table_id              = aws_vpc.sandbox-default-eu-west-1.default_route_table_id
}

resource "aws_route_table" "sandbox-default-private-eu-west-1a" {
  tags = {
    AvailabilityZone = "eu-west-1a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-eu-west-1a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_route_table" "sandbox-default-private-eu-west-1b" {
  tags = {
    AvailabilityZone = "eu-west-1b"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-eu-west-1b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_route_table" "sandbox-default-private-eu-west-1c" {
  tags = {
    AvailabilityZone = "eu-west-1c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-eu-west-1c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_route_table_association" "sandbox-default-private-eu-west-1a" {
  route_table_id = aws_route_table.sandbox-default-private-eu-west-1a.id
  subnet_id      = aws_subnet.sandbox-default-private-eu-west-1a.id
}

resource "aws_route_table_association" "sandbox-default-private-eu-west-1b" {
  route_table_id = aws_route_table.sandbox-default-private-eu-west-1b.id
  subnet_id      = aws_subnet.sandbox-default-private-eu-west-1b.id
}

resource "aws_route_table_association" "sandbox-default-private-eu-west-1c" {
  route_table_id = aws_route_table.sandbox-default-private-eu-west-1c.id
  subnet_id      = aws_subnet.sandbox-default-private-eu-west-1c.id
}

resource "aws_route_table_association" "sandbox-default-public-eu-west-1a" {
  route_table_id = aws_vpc.sandbox-default-eu-west-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-eu-west-1a.id
}

resource "aws_route_table_association" "sandbox-default-public-eu-west-1b" {
  route_table_id = aws_vpc.sandbox-default-eu-west-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-eu-west-1b.id
}

resource "aws_route_table_association" "sandbox-default-public-eu-west-1c" {
  route_table_id = aws_vpc.sandbox-default-eu-west-1.default_route_table_id
  subnet_id      = aws_subnet.sandbox-default-public-eu-west-1c.id
}

resource "aws_subnet" "sandbox-default-private-eu-west-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.cidr_block, 2, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.ipv6_cidr_block, 8, 129)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "eu-west-1a"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-eu-west-1a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_subnet" "sandbox-default-private-eu-west-1b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1b"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.cidr_block, 2, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.ipv6_cidr_block, 8, 130)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "eu-west-1b"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-eu-west-1b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_subnet" "sandbox-default-private-eu-west-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.cidr_block, 2, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.ipv6_cidr_block, 8, 131)
  map_public_ip_on_launch         = false
  tags = {
    AvailabilityZone = "eu-west-1c"
    Connectivity     = "private"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-private-eu-west-1c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_subnet" "sandbox-default-public-eu-west-1a" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1a"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.cidr_block, 4, 1)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.ipv6_cidr_block, 8, 1)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "eu-west-1a"
    Connectivity     = "public"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-public-eu-west-1a"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_subnet" "sandbox-default-public-eu-west-1b" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1b"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.cidr_block, 4, 2)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.ipv6_cidr_block, 8, 2)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "eu-west-1b"
    Connectivity     = "public"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-public-eu-west-1b"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_subnet" "sandbox-default-public-eu-west-1c" {
  assign_ipv6_address_on_creation = true
  availability_zone               = "eu-west-1c"
  cidr_block                      = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.cidr_block, 4, 3)
  ipv6_cidr_block                 = cidrsubnet(aws_vpc.sandbox-default-eu-west-1.ipv6_cidr_block, 8, 3)
  map_public_ip_on_launch         = true
  tags = {
    AvailabilityZone = "eu-west-1c"
    Connectivity     = "public"
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default-public-eu-west-1c"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

resource "aws_vpc" "sandbox-default-eu-west-1" {
  assign_generated_ipv6_cidr_block = true
  cidr_block                       = "10.2.64.0/18"
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

resource "aws_vpc_endpoint" "sandbox-default-eu-west-1" {
  route_table_ids = [
    aws_vpc.sandbox-default-eu-west-1.default_route_table_id,
    aws_route_table.sandbox-default-private-eu-west-1a.id,
    aws_route_table.sandbox-default-private-eu-west-1b.id,
    aws_route_table.sandbox-default-private-eu-west-1c.id,
  ]
  service_name = "com.amazonaws.eu-west-1.s3"
  tags = {
    Environment      = "sandbox"
    Manager          = "Terraform"
    Name             = "sandbox-default"
    Quality          = "default"
    SubstrateVersion = "2021.06"
  }
  vpc_id = aws_vpc.sandbox-default-eu-west-1.id
}

data "aws_organizations_organization" "current" {
}
