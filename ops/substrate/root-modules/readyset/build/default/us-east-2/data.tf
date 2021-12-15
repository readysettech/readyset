
data "aws_region" "current" {}
data "aws_kms_key" "s3-default" {
  key_id = "alias/aws/s3"
}
data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "ci-benchmarking" {
  count = var.benchmarking_iam_role_enabled ? 1 : 0
  statement {
    effect = "Allow"
    sid    = "S3Grants"
    actions = [
      "s3:GetObject*",
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:PutObjectAcl",
      "s3:AbortMultipartUpload"
    ]

    resources = concat(
      [for bucket in local.benchmarking_s3_buckets_allowed : "arn:aws:s3:::${bucket}/*"],
      [for bucket in local.benchmarking_s3_buckets_allowed : "arn:aws:s3:::${bucket}"],
    )
  }

  statement {
    effect = "Allow"
    sid    = "KMSGrants"

    actions = [
      "kms:DescribeKey",
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:GenerateDataKey*",
      "kms:ReEncrypt*"
    ]
    resources = [data.aws_kms_key.s3-default.arn]
  }
}
