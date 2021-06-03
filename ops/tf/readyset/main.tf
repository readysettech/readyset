# Shared load balancer
resource "aws_lb" "main" {
  name = format("%s-%s", var.deployment, var.env)

  enable_cross_zone_load_balancing = true
  internal                         = true
  load_balancer_type               = "network"
  subnets                          = local.private_subnet_ids

  tags = merge(
    {
      Name = var.deployment
    },
    local.tags
  )
}
