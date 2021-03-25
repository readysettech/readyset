data "aws_ami" "noria_server" {
  owners      = [local.readyset_account_id]
  most_recent = true

  filter {
    name   = "name"
    values = ["noria-server-*"]
  }

  filter {
    name   = "tag:Commit"
    values = [var.noria_version]
  }
}

resource "aws_security_group" "noria_server" {
  name        = "noria_server"
  description = "Allow connection to noria server"
  vpc_id      = var.vpc_id

  ingress {
    description = "Noria leader"
    from_port   = 6033
    to_port     = 6033
    protocol    = "tcp"
    self        = true
    security_groups = concat(
      [aws_security_group.noria_mysql.id],
      var.enable_rds_connector ? [
        aws_security_group.debezium_connector[0].id
      ] : []
    )
  }

  ingress {
    description = "Noria workers"
    from_port   = 10000
    to_port     = 65535
    protocol    = "tcp"
    self        = true
    security_groups = concat(
      [aws_security_group.noria_mysql.id],
      var.enable_rds_connector ? [
        aws_security_group.debezium_connector[0].id
      ] : []
    )
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "noria_server" {
  ami           = data.aws_ami.noria_server.image_id
  instance_type = var.noria_server_instance_type
  key_name      = var.key_name

  user_data = templatefile("${path.module}/files/noria_server_init.sh", {
    deployment         = var.deployment
    zookeeper_ip       = aws_instance.zookeeper.private_ip
    noria_memory_bytes = var.noria_memory_bytes
    quorum             = var.noria_quorum
    shards             = var.noria_shards
  })

  subnet_id = local.subnet_id
  vpc_security_group_ids = concat(
    [aws_security_group.noria_server.id],
    var.extra_security_groups
  )
  associate_public_ip_address = var.associate_public_ip_addresses

  tags = {
    Name = "noria_server"
  }

  ebs_block_device {
    delete_on_termination = false
    device_name           = "/dev/sdf"
    encrypted             = var.encrypt_noria_disk
    volume_size           = var.noria_disk_size_gb
  }
}
