data "aws_iam_policy_document" "installer_s3_assume_role" {

  statement {
    sid    = ""
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

data "aws_iam_policy_document" "installer_s3_policy_document" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:PutObjectAcl",
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::readysettech-orchestrator-us-east-2/",
      "arn:aws:s3:::readysettech-orchestrator-us-east-2/*"
    ]
  }
}

resource "aws_iam_role" "installer_s3" {
  name               = "InstallerS3"
  assume_role_policy = data.aws_iam_policy_document.installer_s3_assume_role.json
  inline_policy {
    name   = "AllowInstallerS3Requirements"
    policy = data.aws_iam_policy_document.installer_s3_policy_document.json
  }
}
output "installer-s3-arn" {
  value       = aws_iam_role.installer_s3.arn
  description = "ARN of the Installer bucket S3 IAM Role"
}
