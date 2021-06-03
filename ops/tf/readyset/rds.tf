resource "aws_db_subnet_group" "db" {
  count = var.create_rds ? 1 : 0

  name       = local.db
  subnet_ids = local.private_subnet_ids

  tags = merge(
    var.tags,
    {
      "Name" = local.db
    },
  )
}

resource "aws_db_instance" "db" {
  count = var.create_rds ? 1 : 0

  identifier = local.db

  allocated_storage       = var.rds_instance_allocated_storage
  storage_type            = "io1"
  iops                    = var.rds_instance_iops
  backup_retention_period = 7
  db_subnet_group_name    = aws_db_subnet_group.db[0].name
  engine                  = "mysql"
  engine_version          = var.rds_engine_version
  instance_class          = var.rds_instance_type
  kms_key_id              = var.rds_volume_kms_key_id
  name                    = local.db_name
  username                = var.db_user
  password                = var.db_password
  port                    = var.db_port
  option_group_name       = aws_db_option_group.db[0].name
  parameter_group_name    = aws_db_parameter_group.db[0].name
  storage_encrypted       = var.encrypt_rds_volume
  skip_final_snapshot     = true
  vpc_security_group_ids  = [aws_security_group.db.id]

  tags = merge(
    {
      Name = local.db
    },
    local.tags
  )
}

resource "aws_db_option_group" "db" {
  count = var.create_rds ? 1 : 0

  name                     = local.db
  option_group_description = format("%s Option Group", local.db)
  engine_name              = "mysql"
  major_engine_version     = var.rds_engine_version

  tags = merge(
    {
      Name = local.db
    },
    local.tags
  )
}

resource "aws_db_parameter_group" "db" {
  count = var.create_rds ? 1 : 0

  name   = local.db
  family = format("mysql%s", var.rds_engine_version)

  parameter {
    name  = "binlog_format"
    value = "ROW"
  }

  parameter {
    name  = "binlog_row_image"
    value = "FULL"
  }

  parameter {
    name         = "enforce_gtid_consistency"
    value        = "ON"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "gtid-mode"
    value        = "ON"
    apply_method = "pending-reboot"
  }

  parameter {
    name  = "session_track_gtids"
    value = "1"
  }

  parameter {
    name  = "session_track_state_change"
    value = "1"
  }

  tags = merge(
    {
      Name = local.db
    },
    local.tags
  )
}
