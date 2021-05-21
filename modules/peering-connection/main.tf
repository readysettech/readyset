# managed by Substrate; do not edit by hand

data "aws_region" "accepter" {
  provider = aws.accepter
}

data "aws_route_table" "accepter-private" {
  count     = var.accepter_environment == "admin" ? 0 : length(data.aws_subnet_ids.accepter-private[0].ids)
  provider  = aws.accepter
  subnet_id = tolist(data.aws_subnet_ids.accepter-private[0].ids)[count.index]
}

data "aws_route_table" "requester-private" {
  count     = var.requester_environment == "admin" ? 0 : length(data.aws_subnet_ids.requester-private[0].ids)
  provider  = aws.requester
  subnet_id = tolist(data.aws_subnet_ids.requester-private[0].ids)[count.index]
}

data "aws_subnet_ids" "accepter-private" {
  count    = var.accepter_environment == "admin" ? 0 : 1
  provider = aws.accepter
  tags = {
    Connectivity = "private"
  }
  vpc_id = data.aws_vpc.accepter.id
}

data "aws_subnet_ids" "requester-private" {
  count    = var.requester_environment == "admin" ? 0 : 1
  provider = aws.requester
  tags = {
    Connectivity = "private"
  }
  vpc_id = data.aws_vpc.requester.id
}

data "aws_vpc" "accepter" {
  provider = aws.accepter
  tags = {
    Environment = var.accepter_environment
    Quality     = var.accepter_quality
  }
}

data "aws_vpc" "requester" {
  provider = aws.requester
  tags = {
    Environment = var.requester_environment
    Quality     = var.requester_quality
  }
}

resource "aws_route" "accepter-private" {
  #count                     = length(data.aws_route_table.accepter-private) # better but "Invalid count argument"
  count                     = var.accepter_environment == "admin" ? 0 : length(data.aws_subnet_ids.accepter-private[0].ids) # avoids "Invalid count argument"
  destination_cidr_block    = data.aws_vpc.requester.cidr_block
  provider                  = aws.accepter
  route_table_id            = data.aws_route_table.accepter-private[count.index].id
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.accepter.vpc_peering_connection_id
}

resource "aws_route" "accepter-public" {
  destination_cidr_block    = data.aws_vpc.requester.cidr_block
  provider                  = aws.accepter
  route_table_id            = data.aws_vpc.accepter.main_route_table_id
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.accepter.vpc_peering_connection_id
}

resource "aws_route" "requester-private" {
  #count                     = length(data.aws_route_table.requester-private) # better but "Invalid count argument"
  count                     = var.requester_environment == "admin" ? 0 : length(data.aws_subnet_ids.requester-private[0].ids) # avoids "Invalid count argument"
  destination_cidr_block    = data.aws_vpc.accepter.cidr_block
  provider                  = aws.requester
  route_table_id            = data.aws_route_table.requester-private[count.index].id
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.accepter.vpc_peering_connection_id
}

resource "aws_route" "requester-public" {
  destination_cidr_block    = data.aws_vpc.accepter.cidr_block
  provider                  = aws.requester
  route_table_id            = data.aws_vpc.requester.main_route_table_id
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.accepter.vpc_peering_connection_id
}

resource "aws_vpc_peering_connection" "requester" {
  auto_accept = false
  peer_region = data.aws_region.accepter.name
  peer_vpc_id = data.aws_vpc.accepter.id
  provider    = aws.requester
  tags = {
    Manager = "Terraform"
  }
  vpc_id = data.aws_vpc.requester.id
}

resource "aws_vpc_peering_connection_accepter" "accepter" {
  auto_accept = true
  provider    = aws.accepter
  tags = {
    Manager = "Terraform"
  }
  vpc_peering_connection_id = aws_vpc_peering_connection.requester.id
}

/*
resource "aws_vpc_peering_connection_options" "accepter" {
  provider = aws.accepter
  requester {
    #allow_remote_vpc_dns_resolution = true # not available in case of inter-region peering
  }
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.accepter.id
}

resource "aws_vpc_peering_connection_options" "requester" {
  accepter {
    #allow_remote_vpc_dns_resolution = true # not available in case of inter-region peering
  }
  provider                  = aws.requester
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.accepter.id
}
*/
