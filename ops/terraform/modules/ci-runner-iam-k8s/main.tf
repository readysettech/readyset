#-------------- [ CFN / K8s Benchmarking ] --------------------------- #

module "cfn-iam-role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role"
  version = "4.7.0"

  # General
  create_role       = true
  role_name         = local.iam_role_name
  role_requires_mfa = false

  # Trust and Policy Management
  trusted_role_arns       = local.iam_trusted_role_arns
  custom_role_policy_arns = local.iam_role_custom_policy_arns

  tags = merge(var.resource_tags, {
    Name = local.iam_role_name
    role = "iam_role"
  })
}

resource "aws_iam_policy" "this-cfn" {
  name   = local.iam_pol_name
  path   = "/"
  policy = data.aws_iam_policy_document.cfn-actions.json
}

resource "aws_iam_policy" "assume-role" {
  name   = local.iam_role_assume_pol_name
  path   = "/"
  policy = data.aws_iam_policy_document.grant-assume-role.json
}
