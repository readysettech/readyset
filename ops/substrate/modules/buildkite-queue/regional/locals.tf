locals {
  ssh_key_pair_config = length(var.ssh_key_pair_name) > 0 ? {
        "KeyName" = var.ssh_key_pair_name
  } : {}
  subnet_ids = join(",", var.use_private_subnets ? data.aws_subnets.private_subnets.ids : data.aws_subnets.public_subnets.ids)
}
