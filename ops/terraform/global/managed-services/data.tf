# Necessary to put a VPC in DNS account. Shared VPCs don't work for the initial
# DNS authorization across-account.
data "aws_vpc" "admin-blackhole-network" {
  provider = aws.dns
  tags = {
    Name = "admin-blackhole"
  }
}

data "aws_vpc" "build-default-network" {
  provider = aws.network
  tags = {
    Environment = "build"
    Quality     = "default"
  }
}
