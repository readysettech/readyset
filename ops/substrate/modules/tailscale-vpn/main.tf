#-------------- [ EC2 ] ------------------------------------------- #

resource "aws_instance" "subnet-router" {
  # General
  ami              = local.effective_ami_id
  instance_type    = var.instance_type
  monitoring       = var.enable_detailed_monitoring
  user_data_base64 = base64encode(data.template_file.launch-script.rendered)
  tags = merge(var.resource_tags, {
    Name = local.subnet_router_ec2_name,
    role = "subnet-router"
  })

  # Networking & Security
  associate_public_ip_address = true
  iam_instance_profile        = module.subnet-router-iam-role.iam_instance_profile_name
  key_name                    = var.key_pair_name
  subnet_id                   = sort(data.aws_subnet_ids.public.ids)[0]
  vpc_security_group_ids = [
    aws_security_group.tailscale.id,
  ]

  root_block_device {
    volume_type           = "gp2"
    volume_size           = var.root_volume_configs["volume_size"]
    delete_on_termination = var.root_volume_configs["delete_on_termination"]
    encrypted             = true
    kms_key_id            = data.aws_kms_alias.ebs.target_key_arn
  }
}

resource "aws_eip" "subnet-router" {
  instance = aws_instance.subnet-router.id
  vpc      = true
  tags = merge(var.resource_tags, {
    Name = local.subnet_router_ec2_name,
    role = "subnet-router"
  })
}

#-------------- [ IAM ] ------------------------------------------- #

resource "aws_iam_policy" "subnet-router" {
  name        = local.subnet_router_iam_policy_name
  description = "EC2 instance policy for Tailscale subnet router nodes."
  policy      = data.aws_iam_policy_document.subnet-router.json
}

module "subnet-router-iam-role" {
  source                  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role"
  version                 = "4.7.0"
  create_role             = true
  create_instance_profile = true
  role_name               = local.subnet_router_iam_role_name

  role_requires_mfa     = false
  trusted_role_services = ["ec2.amazonaws.com"]

  custom_role_policy_arns = [
    aws_iam_policy.subnet-router.arn
  ]

  tags = merge(var.resource_tags, {
    Name = local.subnet_router_iam_role_name
    role = "iam_role"
  })
}

#-------------- [ VPC Security ] --------------------------------- #

resource "aws_security_group" "tailscale" {
  name = local.subnet_router_vpc_sg_name

  description = "Security group for Tailscale subnet router instances."
  vpc_id      = var.vpc_id

  # Optionally allow SSH from specified sources
  dynamic "ingress" {
    for_each = var.ssh_access_enabled ? ["enabled"] : []
    content {
      description = "Allow SSH traffic from authorized networks."
      from_port   = var.ssh_port_number
      to_port     = var.ssh_port_number
      protocol    = "tcp"
      cidr_blocks = var.ssh_allowed_ingress_cidrs
    }
  }

  # Outbound *
  egress {
    description = "Allows all outbound traffic."
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.resource_tags, {
    Name = "tailscale-subnet-router",
    role = "tailscale"
  })
}
