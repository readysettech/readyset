data "aws_db_instance" "rds_db" {
  count                  = local.rds_connector_count
  db_instance_identifier = var.rds_instance_id
}

data "aws_ami" "kafka" {
  owners      = [local.readyset_account_id]
  most_recent = true

  filter {
    name   = "name"
    values = ["kafka-*"]
  }
}

resource "aws_security_group" "kafka" {
  count       = local.rds_connector_count
  name        = "kafka"
  description = "Allow connection to kafka"
  vpc_id      = var.vpc_id

  ingress {
    description = "Kafka"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    self        = true
    security_groups = [
      aws_security_group.debezium[0].id,
      aws_security_group.debezium_connector[0].id
    ]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "kafka" {
  count         = local.rds_connector_count
  ami           = data.aws_ami.kafka.image_id
  instance_type = var.kafka_instance_type
  key_name      = var.key_name

  user_data = templatefile("${path.module}/files/kafka_init.sh", {
    zookeeper_ip = aws_instance.zookeeper.private_ip
  })

  subnet_id = local.subnet_id
  vpc_security_group_ids = concat(
    [aws_security_group.kafka[0].id],
    var.extra_security_groups
  )
  associate_public_ip_address = var.associate_public_ip_addresses

  tags = {
    Name = "kafka"
  }

  # TODO(grfn): block device?
}

###

data "aws_ami" "debezium" {
  owners      = [local.readyset_account_id]
  most_recent = true

  filter {
    name   = "name"
    values = ["debezium-*"]
  }
}

resource "aws_security_group" "debezium" {
  count       = local.rds_connector_count
  name        = "debezium"
  description = "Allow connection to debezium"
  vpc_id      = var.vpc_id

  ingress {
    description = "Debezium"
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    self        = true
    security_groups = [
      # nothing right now, but here for documentation of which port we're
      # listening at
    ]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "debezium" {
  count         = local.rds_connector_count
  ami           = data.aws_ami.debezium.image_id
  instance_type = var.debezium_instance_type
  key_name      = var.key_name

  user_data = templatefile("${path.module}/files/debezium_init.sh", {
    kafka_ip         = aws_instance.kafka[0].private_ip,
    db_name          = var.db_name,
    db_user          = var.db_user,
    db_password      = var.db_password,
    db_instance_name = data.aws_db_instance.rds_db[0].db_name,
    db_host          = data.aws_db_instance.rds_db[0].address
  })

  subnet_id = local.subnet_id
  vpc_security_group_ids = concat(
    [aws_security_group.debezium[0].id],
    var.extra_security_groups
  )
  associate_public_ip_address = var.associate_public_ip_addresses

  tags = {
    Name = "debezium"
  }
}

###

data "aws_ami" "debezium_connector" {
  owners      = [local.readyset_account_id]
  most_recent = true

  filter {
    name   = "name"
    values = ["noria-debezium-connector-*"]
  }
}

# Only used so other security groups can allow it
resource "aws_security_group" "debezium_connector" {
  count       = local.rds_connector_count
  name        = "debezium-connector"
  description = "Noria Debezium Connector"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "debezium_connector" {
  count         = local.rds_connector_count
  ami           = data.aws_ami.debezium_connector.image_id
  instance_type = var.debezium_connector_instance_type
  key_name      = var.key_name

  user_data = templatefile("${path.module}/files/debezium_connector_init.sh", {
    tables       = join(",", var.tables)
    kafka_ip     = aws_instance.kafka[0].private_ip,
    zookeeper_ip = aws_instance.zookeeper.private_ip
    db_name      = var.db_name,
    deployment   = var.deployment
  })

  subnet_id = local.subnet_id
  vpc_security_group_ids = concat(
    [aws_security_group.debezium_connector[0].id],
    var.extra_security_groups
  )
  associate_public_ip_address = var.associate_public_ip_addresses

  tags = {
    Name = "debezium-connector"
  }
}
