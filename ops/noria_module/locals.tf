locals {
  rds_connector_count = var.enable_rds_connector ? 1 : 0
  subnet_id           = element(tolist(data.aws_subnet_ids.subnet_ids.ids), 0)
  readyset_account_id = "069491470376"
  db_user             = var.enable_rds_connector && var.db_user == "" ? data.aws_db_instance.rds_db[0].master_username : var.db_user
  db_password         = var.db_password
  db_host             = var.enable_rds_connector && var.db_host == "" ? data.aws_db_instance.rds_db[0].address : var.db_host
  db_port             = var.enable_rds_connector && var.db_port == "" ? data.aws_db_instance.rds_db[0].port : var.db_port
  db_name             = var.enable_rds_connector && var.db_name == "" ? data.aws_db_instance.rds_db[0].db_name : var.db_name
}
