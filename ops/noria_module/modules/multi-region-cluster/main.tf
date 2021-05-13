module "readyset_primary" {
  source = "../.."

  providers = {
    aws = aws.primary
  }

  aws_region       = "us-east-2"
  readyset_version = var.readyset_version
  key_name         = var.key_name
  setup_id         = local.random

  vpc_cidr_block          = "10.0.0.0/16"
  vpc_private_cidr_blocks = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  vpc_public_cidr_blocks  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  allow_ssh                           = true
  mysql_allowed_cidr_blocks           = ["0.0.0.0/0"]
  readyset_server_allowed_cidr_blocks = ["10.0.0.0/16", "10.1.0.0/16"]
  zookeeper_allowed_cidr_blocks       = ["10.0.0.0/16", "10.1.0.0/16"]
}

module "readyset_secondary" {
  source = "../.."

  providers = {
    aws = aws.secondary
  }

  aws_region       = "us-west-2"
  readyset_version = var.readyset_version
  key_name         = var.key_name
  setup_id         = local.random

  vpc_cidr_block          = "10.1.0.0/16"
  vpc_private_cidr_blocks = ["10.1.1.0/24", "10.1.2.0/24", "10.1.3.0/24"]
  vpc_public_cidr_blocks  = ["10.1.101.0/24", "10.1.102.0/24", "10.1.103.0/24"]

  allow_ssh                           = true
  mysql_allowed_cidr_blocks           = ["0.0.0.0/0"]
  readyset_server_allowed_cidr_blocks = ["10.0.0.0/16", "10.1.0.0/16"]
  zookeeper_allowed_cidr_blocks       = ["10.0.0.0/16", "10.1.0.0/16"]
}

module "vpc_peering" {
  source = "git::https://github.com/grem11n/terraform-aws-vpc-peering.git?ref=v3.1.0"

  providers = {
    aws.this = aws.primary
    aws.peer = aws.secondary
  }

  this_vpc_id = module.readyset_primary.vpc_id
  peer_vpc_id = module.readyset_secondary.vpc_id

  auto_accept_peering = true

  tags = {
    Name = "readyset-multi-region-peering"
  }
}
