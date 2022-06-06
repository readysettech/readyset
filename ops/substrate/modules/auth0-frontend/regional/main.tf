data "vercel_team" "readyset" {
  slug = "readyset"
}

data "aws_security_group" "default" {
  vpc_id = var.vpc_id
  name   = "default"
}

resource "aws_acm_certificate" "auth0-frontend" {
  domain_name       = "${var.domain}.readyset.io"
  validation_method = "DNS"
}

resource "vercel_dns" "auth0_frontend_validation" {
  for_each = {
    for dvo in aws_acm_certificate.auth0-frontend.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  domain  = "readyset.io"
  name    = trimsuffix(each.value.name, ".readyset.io.")
  type    = each.value.type
  value   = each.value.record
  team_id = data.vercel_team.readyset.id
}

resource "aws_acm_certificate_validation" "auth0-frontend" {
  certificate_arn = aws_acm_certificate.auth0-frontend.arn
  validation_record_fqdns = [
    for record in vercel_dns.auth0_frontend_validation : "${record.name}.readyset.io"
  ]
}

resource "aws_security_group" "auth0-frontend-instance" {
  name        = "auth0-frontend-instance"
  description = "Security group for the telemetry ingress instances"
  vpc_id      = var.vpc_id

  ingress {
    description     = "HTTP from load balancer"
    from_port       = 80
    to_port         = 80
    protocol        = "tcp"
    security_groups = [aws_security_group.auth0-frontend-lb.id]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "auth0-frontend-instance"
  }
}

resource "random_id" "app_secret" {
  keepers = {
    domain = var.domain
  }
  byte_length = 64
}

module "asg" {
  source  = "terraform-aws-modules/autoscaling/aws"
  version = "~> 6.3.0"
  name    = "auth0-frontend"

  launch_template_name = "auth0-frontend-launch-template"

  image_id        = var.ami_id
  instance_type   = "t2.micro"
  security_groups = [aws_security_group.auth0-frontend-instance.id]
  load_balancers  = [module.elb.elb_id]
  key_name        = var.key_name

  user_data = base64encode(
    <<-EOT
      #!/bin/bash
      set -euo pipefail

      export CLIENT_ID="${var.auth0_domain}"
      export ISSUER_BASE_URL="${var.auth0_domain}"
      export SECRET="${random_id.app_secret.b64_std}"
      export BASE_URL="https://${var.domain}.readyset.io"
      exec /usr/local/bin/user-data-init.sh
    EOT
  )

  block_device_mappings = [{
    device_name = "/dev/xvda"
    no_device   = 0
    ebs = {
      delete_on_termination = true
      volume_size           = 50
      volume_type           = "gp3"
    }
  }]

  metadata_options = {
    # Require IMDSv2
    http_tokens   = "required"
    http_endpoint = "enabled"
  }

  instance_refresh = {
    strategy = "Rolling"
    preferences = {
      checkpoint_delay       = 600
      min_healthy_percentage = 50
    }
  }

  # Auto scaling group
  vpc_zone_identifier = var.subnet_ids
  health_check_type   = "EC2"
  min_size            = 1
  max_size            = var.num_replicas
  desired_capacity    = var.num_replicas
}


resource "aws_security_group" "auth0-frontend-lb" {
  name        = "auth0_frontend"
  description = "Security group for the telemetry ingress load balancer"
  vpc_id      = var.vpc_id

  ingress {
    description      = "HTTP from anywhere"
    from_port        = 80
    to_port          = 80
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  ingress {
    description      = "HTTPS from anywhere"
    from_port        = 443
    to_port          = 443
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

}

module "elb" {
  source  = "terraform-aws-modules/elb/aws"
  version = "~> 3.0"
  name    = "auth0-frontend-elb"

  subnets         = var.subnet_ids
  security_groups = [aws_security_group.auth0-frontend-lb.id]
  internal        = false

  listener = [{
    instance_port      = "80"
    instance_protocol  = "http"
    lb_port            = "443"
    lb_protocol        = "https"
    ssl_certificate_id = aws_acm_certificate_validation.auth0-frontend.certificate_arn
  }]

  health_check = {
    target              = "HTTP:80/"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 2
    timeout             = 5
  }
}

resource "vercel_dns" "main" {
  team_id = data.vercel_team.readyset.id
  domain  = "readyset.io"
  name    = var.domain
  type    = "CNAME"
  value   = "${module.elb.elb_dns_name}."
}
