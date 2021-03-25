data "aws_subnet_ids" "subnet_ids" {
  vpc_id = var.vpc_id
}

locals {
  subnet_id           = element(tolist(data.aws_subnet_ids.subnet_ids.ids), 0)
  readyset_account_id = "069491470376"
}
