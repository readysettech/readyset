module "aws_node_termination_handler_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "~> 4.0"

  create_role      = true
  role_name_prefix = "node-termination-handler"
  role_description = "IRSA role for node termination handler"

  provider_url     = replace(var.cluster_oidc_issuer_url, "https://", "")
  role_policy_arns = [aws_iam_policy.aws_node_termination_handler.arn]
  oidc_fully_qualified_subjects = [
    format("system:serviceaccount:%s:%s", var.helm_deployment_namespace, "aws-node-termination-handler")
  ]
  oidc_fully_qualified_audiences = ["sts.amazonaws.com"]

  tags = var.resource_tags
}

resource "aws_iam_policy" "aws_node_termination_handler" {
  name   = "${local.name}-aws-node-termination-handler"
  policy = data.aws_iam_policy_document.aws_node_termination_handler.json

  tags = var.resource_tags
}

module "aws_node_termination_handler_sqs" {
  source  = "terraform-aws-modules/sqs/aws"
  version = "~> 3.0"

  name                      = local.name
  message_retention_seconds = 300
  policy                    = data.aws_iam_policy_document.aws_node_termination_handler_sqs.json

  tags = var.resource_tags
}

resource "aws_cloudwatch_event_target" "aws_node_termination_handler_asg" {
  target_id = "${local.name}-asg-termination"
  rule      = aws_cloudwatch_event_rule.aws_node_termination_handler_asg.name
  arn       = module.aws_node_termination_handler_sqs.sqs_queue_arn
}

resource "aws_cloudwatch_event_rule" "aws_node_termination_handler_spot" {
  name        = "${local.name}-spot-termination"
  description = "Node termination event rule"
  event_pattern = jsonencode({
    "source" : ["aws.ec2"],
    "detail-type" : ["EC2 Spot Instance Interruption Warning"]
    "resources" : [for group in var.self_managed_node_groups : group.autoscaling_group_arn]
  })
}

resource "aws_cloudwatch_event_target" "aws_node_termination_handler_spot" {
  target_id = "${local.name}-spot-termination"
  rule      = aws_cloudwatch_event_rule.aws_node_termination_handler_spot.name
  arn       = module.aws_node_termination_handler_sqs.sqs_queue_arn
}

# Creating the lifecycle-hook outside of the ASG resource's `initial_lifecycle_hook`
# ensures that node termination does not require the lifecycle action to be completed,
# and thus allows the ASG to be destroyed cleanly.
resource "aws_autoscaling_lifecycle_hook" "aws_node_termination_handler" {
  for_each = var.self_managed_node_groups

  name                   = "aws-node-termination-handler-${each.value.autoscaling_group_name}"
  autoscaling_group_name = each.value.autoscaling_group_name
  lifecycle_transition   = "autoscaling:EC2_INSTANCE_TERMINATING"
  heartbeat_timeout      = 300
  default_result         = "CONTINUE"
}

resource "aws_cloudwatch_event_rule" "aws_node_termination_handler_asg" {
  name        = "${local.name}-asg-termination"
  description = "Node termination event rule"

  event_pattern = jsonencode({
    "source" : ["aws.autoscaling"],
    "detail-type" : ["EC2 Instance-terminate Lifecycle Action"]
    "resources" : [for group in var.self_managed_node_groups : group.autoscaling_group_arn]
  })

  tags = var.resource_tags
}
