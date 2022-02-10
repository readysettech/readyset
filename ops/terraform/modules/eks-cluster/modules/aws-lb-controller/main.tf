#-------------- [ IAM ] -------------------------------------------------- #

resource "aws_iam_role" "aws_lb_controller" {
  name               = format("%s-eks-aws-lb-controller", var.cluster_name)
  assume_role_policy = data.aws_iam_policy_document.aws_lb_controller.json
}

resource "aws_iam_role_policy" "aws-lb-controller-policy" {
  name   = format("%s-eks-aws-lb-controller", var.cluster_name)
  role   = aws_iam_role.aws_lb_controller.id
  policy = templatefile("${path.module}/templates/controller-iam-role-policy.json", {})
}

#-------------- [ Helm ] ------------------------------------------------- #

resource "helm_release" "aws_lb_controller" {
  name       = var.helm_deployment_name
  repository = var.helm_chart_repository
  chart      = var.helm_chart_name
  namespace  = var.helm_deployment_namespace
  version    = var.helm_chart_version
  values = [
    templatefile("${path.module}/templates/helm/values.yaml", {
      cluster_name = var.cluster_name,
      region       = var.aws_region,
      role         = aws_iam_role.aws_lb_controller.arn,
    })
  ]
}
