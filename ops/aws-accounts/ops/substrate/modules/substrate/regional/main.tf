# managed by Substrate; do not edit by hand

data "aws_subnet_ids" "private" {
  count    = module.global.tags.environment == "admin" ? 0 : 1
  provider = aws.network
  tags = {
    Connectivity = "private"
    Environment  = module.global.tags.environment
    Quality      = module.global.tags.quality
  }
  vpc_id = data.aws_vpc.network.id
}

data "aws_subnet_ids" "public" {
  provider = aws.network
  tags = {
    Connectivity = "public"
    Environment  = module.global.tags.environment
    Quality      = module.global.tags.quality
  }
  vpc_id = data.aws_vpc.network.id
}

data "aws_vpc" "network" {
  provider = aws.network
  tags = {
    Environment = module.global.tags.environment
    Quality     = module.global.tags.quality
  }
}

module "global" {
  source = "../global"
}
