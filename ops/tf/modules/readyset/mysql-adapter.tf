resource "aws_launch_template" "mysql_adapter" {
  name = local.mysql_adapter

  ebs_optimized = true
  image_id      = data.aws_ami.mysql_adapter.image_id
  instance_type = var.mysql_adapter_instance_type
  key_name      = var.key_name

  block_device_mappings {
    device_name = "/dev/sda1"

    ebs {
      volume_size = 20
    }
  }

  tag_specifications {
    resource_type = "instance"

    tags = merge(
      {
        Name = local.mysql_adapter
      },
      local.tags
    )
  }

  tag_specifications {
    resource_type = "volume"

    tags = merge(
      {
        Name = local.mysql_adapter
      },
      local.tags
    )
  }

  user_data = base64encode(templatefile("${path.module}/files/mysql-adapter-init.sh", {
    MYSQL_URL     = local.mysql_url
    DEPLOYMENT    = var.deployment
    ZOOKEEPER_URL = join(",", formatlist("%s:2181", aws_instance.zookeeper.*.private_ip))
  }))

  vpc_security_group_ids = concat(
    [
      aws_security_group.mysql_adapter.id,
      aws_security_group.ssh.id
    ],
    var.extra_security_groups
  )
}

resource "aws_autoscaling_group" "main" {
  name = local.mysql_adapter

  desired_capacity = var.mysql_adapter_instance_count
  max_size         = var.mysql_adapter_instance_count
  min_size         = var.mysql_adapter_instance_count

  health_check_grace_period = 120
  health_check_type         = "EC2"
  target_group_arns = [
    aws_lb_target_group.mysql_adapter.arn
  ]
  vpc_zone_identifier = local.private_subnet_ids

  dynamic "tag" {
    for_each = local.mysql_adapter_asg_tags
    content {
      key                 = tag.value.key
      value               = tag.value.value
      propagate_at_launch = tag.value.propagate_at_launch
    }
  }

  launch_template {
    id      = aws_launch_template.mysql_adapter.id
    version = "$Latest"
  }
}

resource "aws_lb_listener" "mysql_adapter" {
  load_balancer_arn = aws_lb.main.arn

  port     = 3306
  protocol = "TCP"

  default_action {
    target_group_arn = aws_lb_target_group.mysql_adapter.arn
    type             = "forward"
  }

  tags = merge(
    {
      Name = local.mysql_adapter
    },
    local.tags
  )
}

resource "aws_lb_target_group" "mysql_adapter" {
  vpc_id   = data.aws_vpc.vpc.id
  port     = 3306
  protocol = "TCP"

  tags = merge(
    {
      Name = local.mysql_adapter
    },
    local.tags
  )
}
