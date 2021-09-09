// This defines a role that is the only role allowed to update artifacts for the
// custoemrs in an automated way. This role should be used only by automation
// which has logging on who started the automation.
data "aws_iam_policy_document" "deploy_customer_artifacts_write_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::305232526136:role/buildkite-Role"]
    }
  }
}

data "aws_iam_policy_document" "deploy_customer_artifacts_write_s3" {
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      "arn:aws:s3:::readysettech-customer-artifacts-us-east-2",
      "arn:aws:s3:::readysettech-customer-artifacts-us-east-2/*"
    ]
  }
}

resource "aws_iam_role" "deploy_customer_artifacts_write" {
  name = "DeployCustomerArtifactsWrite"

  assume_role_policy = data.aws_iam_policy_document.deploy_customer_artifacts_write_assume_role.json

  inline_policy {
    name   = "DeployCustomerArtifactsWriteS3"
    policy = data.aws_iam_policy_document.deploy_customer_artifacts_write_s3.json
  }
}
