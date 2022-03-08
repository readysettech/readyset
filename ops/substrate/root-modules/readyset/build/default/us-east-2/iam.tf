
#-------------- [ Benchmarking ] -------------------------------------- #

module "ci-benchmarking-iam-role" {
  count   = var.benchmarking_iam_role_enabled ? 1 : 0
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role"
  version = "4.7.0"

  # General
  create_role       = var.benchmarking_iam_role_enabled
  role_name         = local.benchmarking_iam_role_name
  role_requires_mfa = false

  # Trust and Policy Management
  trusted_role_arns       = local.benchmarking_iam_trusted_role_arns
  custom_role_policy_arns = local.benchmarking_iam_role_custom_policy_arns

  tags = merge(var.resource_tags, {
    Name = local.benchmarking_iam_role_name
    role = "iam_role"
  })
}

resource "aws_iam_policy" "ci-benchmarking" {
  count  = var.benchmarking_iam_role_enabled ? 1 : 0
  name   = local.benchmarking_iam_pol_name
  path   = "/"
  policy = data.aws_iam_policy_document.ci-benchmarking[0].json
}

# Policy to allow Buildkite queue agents to assume role
resource "aws_iam_policy" "bk-benchmarking-assume-role" {
  count  = var.benchmarking_iam_role_enabled ? 1 : 0
  name   = local.bk_benchmarking_iam_pol_name
  path   = "/"
  policy = data.aws_iam_policy_document.bk-benchmarking-assume-role[0].json
}

# Policy to allow Buildkite agents to assume role that provides a level
# of k8s access
resource "aws_iam_policy" "bk-k8s-assume-role" {
  count  = var.buildkite_k8s_queue_iam_role_enabled ? 1 : 0
  name   = local.bk_k8s_role_iam_pol_name
  path   = "/"
  policy = data.aws_iam_policy_document.bk-k8s-assume-role[0].json
}
