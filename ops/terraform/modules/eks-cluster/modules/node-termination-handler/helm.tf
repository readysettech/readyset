
################################################################################
# Node Termination Handler
# Based on the official docs at
# https://github.com/aws/aws-node-termination-handler
################################################################################

resource "helm_release" "aws_node_termination_handler" {
  name             = var.helm_deployment_name
  namespace        = var.helm_deployment_namespace
  repository       = var.helm_chart_repository
  chart            = var.helm_chart_name
  version          = var.helm_chart_version
  create_namespace = false

  set {
    name  = "awsRegion"
    value = var.aws_region
  }

  set {
    name  = "serviceAccount.name"
    value = "aws-node-termination-handler"
  }

  set {
    name  = "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn"
    value = module.aws_node_termination_handler_role.iam_role_arn
    type  = "string"
  }

  set {
    name  = "enableSqsTerminationDraining"
    value = "true"
  }

  set {
    name  = "enableSpotInterruptionDraining"
    value = "true"
  }

  set {
    name  = "queueURL"
    value = module.aws_node_termination_handler_sqs.sqs_queue_id
  }

  set {
    name  = "logLevel"
    value = "debug"
  }
}
