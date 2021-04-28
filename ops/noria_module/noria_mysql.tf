data "aws_ami" "noria_mysql" {
  owners      = [local.readyset_account_id]
  most_recent = true

  filter {
    name   = "name"
    values = ["noria_mysql-*"]
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

resource "aws_security_group" "noria_mysql" {
  name        = "noria_mysql"
  description = "Allow connection to mysql adapter"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "MySQL"
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    cidr_blocks = var.mysql_allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "noria_mysql" {
  ami           = data.aws_ami.noria_mysql.image_id
  instance_type = var.noria_mysql_instance_type
  key_name      = var.key_name

  user_data = templatefile("${path.module}/files/noria_mysql_init.sh", {
    deployment              = var.deployment
    zookeeper_ip            = aws_instance.zookeeper.private_ip
    mysql_connection_string = "mysql://${local.db_user}:${local.db_password}@${local.db_host}:${local.db_port}/${local.db_name}"
  })

  subnet_id = local.subnet_id
  vpc_security_group_ids = concat(
    [aws_security_group.noria_mysql.id],
    local.extra_security_groups
  )
  associate_public_ip_address = var.associate_public_ip_addresses

  tags = {
    Name = "noria_mysql"
  }
}
