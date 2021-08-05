data "aws_vpc" "vpc" {
  tags = {
    Name = "sandbox-default"
  }
}

resource "random_password" "db_password" {
  length = 16
}

resource "random_string" "identifier" {
  length  = 16
  special = false
  upper   = false
  number  = false
}

module "readyset_tmp" {
  source = "../../modules/readyset"

  env = "tmp"

  readyset_version = var.readyset_commit
  vpc              = "sandbox-default"
  deployment       = format("readyset-tmp-%s-%s", var.readyset_commit == "*" ? "latest" : var.readyset_commit, random_string.identifier.result)

  // TODO: Create a readyset tmp key that we can share more broadly for this usecase. For now, use the fastly key.
  key_name  = "readyset-fastly"
  allow_ssh = true

  create_rds  = true
  db_password = random_password.db_password.result

  mysql_adapter_allowed_cidr_blocks = [data.aws_vpc.vpc.cidr_block]
  mysql_adapter_instance_type       = "c5.4xlarge"
  rds_allowed_cidr_blocks           = [data.aws_vpc.vpc.cidr_block]
  rds_instance_type                 = "db.m5.large"
  server_allowed_cidr_blocks        = [data.aws_vpc.vpc.cidr_block]
  server_instance_type              = "c5.4xlarge"
  zookeeper_instance_count          = 3
}
