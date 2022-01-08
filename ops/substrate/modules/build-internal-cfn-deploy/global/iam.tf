data "aws_iam_policy_document" "internal-cfn-ci-assume-role-document" {

  statement {
    sid = ""
    effect = "Allow"
    actions = [
      "sts:AssumeRole"
    ]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::305232526136:role/buildkite-ops-Role"]
    }
  }

}

data "aws_iam_policy_document" "internal-cfn-access-policy-role-document" {

  statement {
    sid    = ""
    effect = "Allow"
    # Note these values will need to be manually copied from the AWS console :c
    resources = [
      "*"
    ]
    actions = [
      "cloudformation:Describe*",
      "cloudformation:EstimateTemplateCost",
      "cloudformation:Get*",
      "cloudformation:List*",
      "cloudformation:ValidateTemplate",
      "cloudformation:Detect*",
      "cloudformation:CreateStack",
      "cloudformation:DeleteStack",
      "s3:getObject",
      "s3:ListBucket",
      "ssm:GetParameter"
    ]
  }

}

resource "aws_iam_role" "internal-cfn-ci" {

  name = "InternalCloudFormationCI"
  assume_role_policy = data.aws_iam_policy_document.internal-cfn-ci-assume-role-document.json
  inline_policy {
    name = "InternalCFNCIPolicy"
    policy = data.aws_iam_policy_document.internal-cfn-access-policy-role-document.json
  }

}

output "internal-cfn-ci-arn" {
  value = aws_iam_role.internal-cfn-ci.arn
  description = "ARN of the Internal CFN CI IAM Role"
}
