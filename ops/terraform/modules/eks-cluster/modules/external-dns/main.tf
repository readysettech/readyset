#-------------- [ IAM ] -------------------------------------------------- #

resource "aws_iam_role" "externaldns" {
  name               = format("%s-%s", local.name, var.cluster_id)
  assume_role_policy = data.aws_iam_policy_document.externaldns.json
}

resource "aws_iam_role_policy" "externaldns" {
  name   = format("%s-%s", local.name, var.cluster_id)
  role   = aws_iam_role.externaldns.id
  policy = data.aws_iam_policy_document.externaldns-policy.json
}

#-------------- [ Helm ] ------------------------------------------------- #

resource "helm_release" "externaldns" {
  name       = local.name
  repository = var.helm_chart_repository
  chart      = var.helm_chart_name
  namespace  = var.helm_deployment_namespace
  version    = var.helm_chart_version
  values     = [data.template_file.externaldns.rendered]
}
