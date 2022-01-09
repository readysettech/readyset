locals {
  # Debian 10 with some manual configuration, for now - later, will replace this
  # with a prebuilt AMI
  docs_ami = "ami-0de4285eac39bd31f"
  key_name = "docs"
}

data "aws_vpc" "docs_vpc" {
  tags = {
    Name = "build-default"
  }
}

data "aws_subnet" "docs_subnet" {
  vpc_id            = data.aws_vpc.docs_vpc.id
  availability_zone = "us-east-2a"

  filter {
    name   = "tag:Connectivity"
    values = ["private"]
  }
}

resource "aws_security_group" "docs" {
  name        = "docs"
  description = "Security group for the docs instance"
  vpc_id      = data.aws_vpc.docs_vpc.id
}

resource "aws_security_group_rule" "docs_all_egress" {
  security_group_id = aws_security_group.docs.id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "all"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "docs_ssh_from_vpc" {
  security_group_id = aws_security_group.docs.id
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "TCP"
  cidr_blocks       = [for cba in data.aws_vpc.docs_vpc.cidr_block_associations : cba.cidr_block]
}

resource "aws_instance" "docs" {
  ami                    = local.docs_ami
  instance_type          = "t3a.small"
  key_name               = local.key_name
  subnet_id              = data.aws_subnet.docs_subnet.id
  vpc_security_group_ids = [resource.aws_security_group.docs.id]

  tags = { Name = "docs" }
}
