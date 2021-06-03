locals {
  ami_account_id     = "069491470376"
  device_name        = "/dev/xvdd"
  private_subnet_ids = length(var.private_subnet_ids) == 0 ? tolist(data.aws_subnet_ids.private[0].ids) : var.private_subnet_ids

  # Database settings
  db_host = var.create_rds ? aws_db_instance.db[0].address : var.db_host
  db_name = coalesce(var.db_name, var.env)

  mysql_url = format("mysql://%s:%s@%s:%s/%s",
    var.db_user,
    var.db_password,
    local.db_host,
    var.db_port,
    local.db_name,
  )

  # Resource names
  db            = format("%s-%s", var.deployment, var.env)
  mysql_adapter = format("%s-%s-mysql-adapter", var.deployment, var.env)
  server        = format("%s-%s-server", var.deployment, var.env)
  zookeeper     = format("%s-%s-zookeeper", var.deployment, var.env)

  # Tags
  tags = merge(
    {
      Deployment  = var.deployment
      Environment = var.env
    },
    var.tags,
  )

  asg_tags = flatten([[
    for k, v in local.tags : {
      key                 = k
      value               = v
      propagate_at_launch = true
    }
  ]])
}
