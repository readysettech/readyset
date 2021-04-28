data "aws_ami" "zookeeper" {
  owners      = [local.readyset_account_id]
  most_recent = true

  filter {
    name   = "name"
    values = ["zookeeper-*"]
  }

  filter {
    name   = "tag:Built_with"
    values = ["Packer"]
  }

  filter {
    name   = "tag:Commit_ID"
    values = [var.readyset_version]
  }
}

resource "aws_security_group" "zookeeper" {
  name        = "zookeeper"
  description = "Allow connection to zookeeper"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Zookeeper Peerport"
    from_port   = 2888
    to_port     = 2888
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Zookeeper Leaderport"
    from_port   = 3888
    to_port     = 3888
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Zookeeper Clientport"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    security_groups = concat(
      [
        aws_security_group.noria_server.id,
        aws_security_group.noria_mysql.id
      ],
      var.enable_rds_connector ? [
        aws_security_group.kafka[0].id,
        aws_security_group.debezium[0].id,
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

resource "aws_instance" "zookeeper" {
  ami           = data.aws_ami.zookeeper.image_id
  instance_type = var.zookeeper_instance_type
  key_name      = var.key_name

  subnet_id = local.subnet_id
  vpc_security_group_ids = concat(
    [aws_security_group.zookeeper.id],
    local.extra_security_groups,
  )
  associate_public_ip_address = var.associate_public_ip_addresses

  tags = {
    Name = "zookeeper"
  }

  user_data = templatefile("${path.module}/files/zookeeper_init.sh", {
    device_name = "/dev/xvdd"
  })
}

resource "aws_ebs_volume" "zookeeper" {
  availability_zone = aws_instance.zookeeper.availability_zone
  type              = "gp2"
  size              = var.zookeeper_disk_size_gb
  encrypted         = var.encrypt_zookeeper_disk
  kms_key_id        = var.zookeeper_disk_kms_key_id
}

resource "aws_volume_attachment" "zookeeper" {
  device_name = "/dev/xvdd"
  instance_id = aws_instance.zookeeper.id
  volume_id   = aws_ebs_volume.zookeeper.id
}
