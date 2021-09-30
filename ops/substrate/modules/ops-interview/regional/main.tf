locals {
  name          = "${var.candidate_name} Interview"
  debian_10_ami = "ami-089fe97bc00bff7cc"
}

data "aws_region" "current" {}

resource "aws_vpc" "ops_interview" {
  cidr_block = "172.16.0.0/16"
  tags       = { Name = local.name }
}

resource "aws_subnet" "ops_interview" {
  vpc_id            = aws_vpc.ops_interview.id
  cidr_block        = "172.16.10.0/24"
  availability_zone = "${data.aws_region.current.name}a"
  tags              = { Name = local.name }
}

resource "aws_security_group" "ops_interview" {
  name        = local.name
  description = "Security group for ${var.candidate_name} interview"
  vpc_id      = aws_vpc.ops_interview.id
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.ops_interview.id
  tags   = { Name = "gw" }
}

resource "aws_default_route_table" "route_table" {
  default_route_table_id = aws_vpc.ops_interview.default_route_table_id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_security_group_rule" "all_egress" {
  security_group_id = aws_security_group.ops_interview.id
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "all"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_security_group_rule" "candidate_ssh" {
  security_group_id = aws_security_group.ops_interview.id
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["${var.candidate_ip_address}/32"]
}

resource "aws_security_group_rule" "extra_ssh" {
  for_each          = toset(var.extra_allowed_ips)
  security_group_id = aws_security_group.ops_interview.id
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["${each.key}/32"]
}

resource "aws_security_group_rule" "http" {
  security_group_id = aws_security_group.ops_interview.id
  type              = "ingress"
  from_port         = 80
  to_port           = 80
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_instance" "ops_interview" {
  ami           = local.debian_10_ami
  instance_type = "t3.micro"

  key_name               = var.interviewer_key_pair_name
  subnet_id              = aws_subnet.ops_interview.id
  vpc_security_group_ids = [aws_security_group.ops_interview.id]

  associate_public_ip_address = true

  tags = { Name = local.name }

  user_data = <<-EOT
#!/bin/bash
set -euo pipefail
echo "${var.candidate_ssh_public_key}" >> /home/admin/.ssh/authorized_keys
EOT
}
