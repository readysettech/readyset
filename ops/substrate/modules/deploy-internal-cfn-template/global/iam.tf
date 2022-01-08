data "aws_iam_policy_document" "internal-artifacts-access-assume-role-document" {

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

data "aws_iam_policy_document" "internal-artifacts-access-policy-role-document" {

  statement {
    sid    = ""
    effect = "Allow"
    # These resources should point to the Readyset Internal CFN Bucket and Bucket Objects in some number of regions
    # Note these values will need to be manually copied from the AWS console :c
    resources = [
      "arn:aws:s3:::readysettech-cfn-internal-us-east-2",
      "arn:aws:s3:::readysettech-cfn-internal-us-east-2/*"
    ]
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket"
    ]
  }

}

resource "aws_iam_role" "internal-artifacts-write" {

  name = "InternalArtifactsWrite"
  assume_role_policy = data.aws_iam_policy_document.internal-artifacts-access-assume-role-document.json

}

resource "aws_iam_role_policy" "internal-artifacts-write-role-policy" {
  policy = data.aws_iam_policy_document.internal-artifacts-access-policy-role-document.json
  role   = aws_iam_role.internal-artifacts-write.id
  name   = "InternalArtifactsWriteRolePolicy"
}

output "internal-artifacts-write-arn" {
  value = aws_iam_role.internal-artifacts-write.arn
  description = "ARN of the Internal Artifacts Write IAM Role"
}