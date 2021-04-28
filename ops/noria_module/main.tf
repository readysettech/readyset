resource "random_string" "random" {
  length  = 5
  special = false
}

resource "aws_security_group" "ssh" {
  count       = var.allow_ssh ? 1 : 0
  name        = "allow-ssh"
  description = "Allow connections to SSH"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description      = "SSH"
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}
