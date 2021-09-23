locals {
  readysettech_customer_artifacts_bucket_name = "readysettech-customer-artifacts-${data.aws_region.current.name}"
  readysettech_customer_artifacts_bucket_arn  = "arn:aws:s3:::${local.readysettech_customer_artifacts_bucket_name}"
  readysettech_customer_artifacts_objects_arn = "${local.readysettech_customer_artifacts_bucket_arn}/*"


}

data "aws_region" "current" {}

data "aws_iam_policy_document" "readysettech-customer-artifacts" {
  statement {
    sid    = "AllowDeployCustomerArtifactsWrite"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::305232526136:role/DeployCustomerArtifactsWrite"]
    }
    resources = [
      "${local.readysettech_customer_artifacts_bucket_arn}",
      "${local.readysettech_customer_artifacts_objects_arn}"
    ]
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket"
    ]
  }
  statement {
    sid    = "AllowReadysetRead"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::069491470376:root", "arn:aws:iam::716876017850:root"]
    }
    resources = [
      "${local.readysettech_customer_artifacts_bucket_arn}",
      "${local.readysettech_customer_artifacts_objects_arn}"
    ]
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
  }
  statement {
    sid     = "AllowTLSRequestsOnly"
    actions = ["s3:*"]
    effect  = "Deny"
    resources = [
      "${local.readysettech_customer_artifacts_bucket_arn}",
      "${local.readysettech_customer_artifacts_objects_arn}"
    ]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket" "readysettech-customer-artifacts" {
  bucket = local.readysettech_customer_artifacts_bucket_name
  tags = {
    Name = local.readysettech_customer_artifacts_bucket_name
  }
  versioning {
    enabled = true
  }
}

resource "aws_s3_bucket_policy" "readysettech-customer-artifacts" {
  bucket = aws_s3_bucket.readysettech-customer-artifacts.bucket
  policy = data.aws_iam_policy_document.readysettech-customer-artifacts.json
}

resource "aws_s3_bucket_public_access_block" "readysettech-customer-artifacts" {
  block_public_acls       = true
  block_public_policy     = true
  bucket                  = aws_s3_bucket.readysettech-customer-artifacts.bucket
  ignore_public_acls      = true
  restrict_public_buckets = true
}


