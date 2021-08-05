resource "aws_security_group" "ssh" {
  name        = format("%s-%s-ssh", var.deployment, var.env)
  description = "Allow SSH connections"
  vpc_id      = data.aws_vpc.vpc.id

  tags = merge(
    {
      Name = format("%s-%s-ssh", var.deployment, var.env)
    },
    local.tags
  )
}

resource "aws_security_group_rule" "ssh_ingress" {
  count = length(var.ssh_allowed_cidr_blocks) > 0 ? 1 : 0

  security_group_id = aws_security_group.ssh.id

  type      = "ingress"
  from_port = 22
  to_port   = 22
  protocol  = "tcp"

  cidr_blocks = var.ssh_allowed_cidr_blocks
}

resource "aws_security_group_rule" "ssh_ingress_vpc" {
  count = var.allow_ssh && length(var.ssh_allowed_cidr_blocks) == 0 ? 1 : 0

  security_group_id = aws_security_group.ssh.id

  type      = "ingress"
  from_port = 22
  to_port   = 22
  protocol  = "tcp"

  cidr_blocks = [data.aws_vpc.vpc.cidr_block]
}

resource "aws_security_group" "db" {
  name        = format("%s-db", local.db)
  description = "Allow connections to the RDS instance"
  vpc_id      = data.aws_vpc.vpc.id

  tags = merge(
    {
      Name = format("%s-db", local.db)
    },
    local.tags
  )
}

resource "aws_security_group_rule" "db_ingress_mysql_mysql_adapter" {
  security_group_id = aws_security_group.db.id

  type      = "ingress"
  from_port = var.db_port
  to_port   = var.db_port
  protocol  = "tcp"

  source_security_group_id = aws_security_group.mysql_adapter.id
}

resource "aws_security_group_rule" "db_ingress_mysql_server" {
  security_group_id = aws_security_group.db.id

  type      = "ingress"
  from_port = var.db_port
  to_port   = var.db_port
  protocol  = "tcp"

  source_security_group_id = aws_security_group.server.id
}


resource "aws_security_group_rule" "db_ingress_mysql_cidr_blocks" {
  count = length(var.rds_allowed_cidr_blocks) > 0 ? 1 : 0

  security_group_id = aws_security_group.db.id

  type      = "ingress"
  from_port = var.db_port
  to_port   = var.db_port
  protocol  = "tcp"

  cidr_blocks = var.rds_allowed_cidr_blocks
}

resource "aws_security_group" "mysql_adapter" {
  name        = local.mysql_adapter
  description = "Allow connections to the MySQL adapter"
  vpc_id      = data.aws_vpc.vpc.id

  egress {
    from_port = 0
    to_port   = 0
    protocol  = "all"

    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    {
      Name = local.mysql_adapter
    },
    local.tags
  )
}

resource "aws_security_group_rule" "mysql_adapter_ingress_mysql_cidr_blocks" {
  count = length(var.mysql_adapter_allowed_cidr_blocks) > 0 ? 1 : 0

  security_group_id = aws_security_group.mysql_adapter.id

  type      = "ingress"
  from_port = 3306
  to_port   = 3306
  protocol  = "tcp"

  cidr_blocks = var.mysql_adapter_allowed_cidr_blocks
}

resource "aws_security_group" "server" {
  name        = local.server
  description = "Allow connection to the server"
  vpc_id      = data.aws_vpc.vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    {
      Name = local.server
    },
    local.tags
  )
}

resource "aws_security_group_rule" "server_ingress_leader" {
  security_group_id = aws_security_group.server.id

  type      = "ingress"
  from_port = 6033
  to_port   = 6033
  protocol  = "tcp"

  self = true
}

resource "aws_security_group_rule" "server_ingress_leader_mysql_adapter" {
  security_group_id = aws_security_group.server.id

  type      = "ingress"
  from_port = 6033
  to_port   = 6033
  protocol  = "tcp"

  source_security_group_id = aws_security_group.mysql_adapter.id
}

resource "aws_security_group_rule" "server_ingress_leader_cidr_blocks" {
  count = length(var.server_allowed_cidr_blocks) > 0 ? 1 : 0

  security_group_id = aws_security_group.server.id

  type      = "ingress"
  from_port = 6033
  to_port   = 6033
  protocol  = "tcp"

  cidr_blocks = var.server_allowed_cidr_blocks
}

resource "aws_security_group_rule" "server_ingress_worker" {
  security_group_id = aws_security_group.server.id

  type      = "ingress"
  from_port = 32768 # https://en.wikipedia.org/wiki/Ephemeral_port
  to_port   = 61000
  protocol  = "tcp"

  self = true
}

resource "aws_security_group_rule" "server_ingress_worker_mysql_adapter" {
  security_group_id = aws_security_group.server.id

  type      = "ingress"
  from_port = 32768 # https://en.wikipedia.org/wiki/Ephemeral_port
  to_port   = 61000
  protocol  = "tcp"

  source_security_group_id = aws_security_group.mysql_adapter.id
}

resource "aws_security_group_rule" "server_ingress_worker_cidr_blocks" {
  count = length(var.server_allowed_cidr_blocks) > 0 ? 1 : 0

  security_group_id = aws_security_group.server.id

  type      = "ingress"
  from_port = 32768 # https://en.wikipedia.org/wiki/Ephemeral_port
  to_port   = 61000
  protocol  = "tcp"

  cidr_blocks = var.server_allowed_cidr_blocks
}

resource "aws_security_group_rule" "server_ingress_prometheus_cidr_blocks" {
  count = length(var.server_allowed_cidr_blocks) > 0 ? 1 : 0

  security_group_id = aws_security_group.server.id

  type      = "ingress"
  from_port = 9100
  to_port   = 9100
  protocol  = "tcp"

  cidr_blocks = var.server_allowed_cidr_blocks
}

resource "aws_security_group" "zookeeper" {
  name        = local.zookeeper
  description = "Allow connection to zookeeper"
  vpc_id      = data.aws_vpc.vpc.id

  egress {
    from_port = 0
    to_port   = 0
    protocol  = "all"

    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    {
      Name = local.zookeeper
    },
    local.tags
  )
}

resource "aws_security_group_rule" "zookeeper_ingress_client_mysql_adapter" {
  security_group_id = aws_security_group.zookeeper.id

  type      = "ingress"
  from_port = 2181
  to_port   = 2181
  protocol  = "tcp"

  source_security_group_id = aws_security_group.mysql_adapter.id
}

resource "aws_security_group_rule" "zookeeper_ingress_client_server" {
  security_group_id = aws_security_group.zookeeper.id

  type      = "ingress"
  from_port = 2181
  to_port   = 2181
  protocol  = "tcp"

  source_security_group_id = aws_security_group.server.id
}

resource "aws_security_group_rule" "zookeeper_ingress_client_cidr_blocks" {
  count = length(var.zookeeper_allowed_cidr_blocks) > 0 ? 1 : 0

  security_group_id = aws_security_group.zookeeper.id

  type      = "ingress"
  from_port = 2181
  to_port   = 2181
  protocol  = "tcp"

  cidr_blocks = var.zookeeper_allowed_cidr_blocks
}

resource "aws_security_group_rule" "zookeeper_ingress_peer" {
  security_group_id = aws_security_group.zookeeper.id

  type      = "ingress"
  from_port = 2888
  to_port   = 2888
  protocol  = "tcp"

  self = true
}

resource "aws_security_group_rule" "zookeeper_ingress_leader" {
  security_group_id = aws_security_group.zookeeper.id

  type      = "ingress"
  from_port = 3888
  to_port   = 3888
  protocol  = "tcp"

  self = true
}
