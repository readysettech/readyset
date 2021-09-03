// This defines a role that is the only role allowed to push into ECR in the
// Deploy account. This role should be used only by very specific Buildkite jobs
// that have additional auditing as we will be sharing access to the ECR with
// customers
data "aws_iam_policy_document" "push_deploy_ecr_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::305232526136:role/buildkite-Role"]
    }
  }
}

data "aws_iam_policy_document" "push_deploy_ecr" {
  statement {
    effect = "Allow"
    actions = [
      "ecr:CompleteLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:InitiateLayerUpload",
      "ecr:BatchCheckLayerAvailability",
      "ecr:PutImage"
    ]
    resources = [
      "arn:aws:ecr:us-east-2:888984949675:repository/*"
    ]
  }
  statement {
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
}

resource "aws_iam_role" "push_deploy_ecr" {
  name = "PushDeployECR"

  assume_role_policy = data.aws_iam_policy_document.push_deploy_ecr_assume_role.json

  inline_policy {
    name   = "PushDeployECR"
    policy = data.aws_iam_policy_document.push_deploy_ecr.json
  }
}
