data "aws_subnet_ids" "subnet_ids" {
  vpc_id = var.vpc_id
}

data "aws_subnet" "subnets" {
  for_each = data.aws_subnet_ids.subnet_ids.ids
  id       = each.value
}

locals {
  subnet_id           = element(tolist(data.aws_subnet_ids.subnet_ids.ids), 0)
  availability_zone   = element(values(data.aws_subnet.subnets), 0).availability_zone
  readyset_account_id = "069491470376"
}
