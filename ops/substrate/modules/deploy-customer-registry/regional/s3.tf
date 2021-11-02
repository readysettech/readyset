locals {
  readysettech_customer_artifacts_bucket_name = "readysettech-customer-artifacts-${data.aws_region.current.name}"
  readysettech_customer_artifacts_bucket_arn  = "arn:aws:s3:::${local.readysettech_customer_artifacts_bucket_name}"
  readysettech_customer_artifacts_objects_arn = "${local.readysettech_customer_artifacts_bucket_arn}/*"

  readysettech_cfn_public_bucket_name = "readysettech-cfn-public-${data.aws_region.current.name}"
  readysettech_cfn_public_bucket_arn  = "arn:aws:s3:::${local.readysettech_cfn_public_bucket_name}"
  readysettech_cfn_public_objects_arn = "${local.readysettech_cfn_public_bucket_arn}/*"
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
  bucket = aws_s3_bucket.readysettech-customer-artifacts.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "readysettech-cfn-public" {
  statement {
    sid    = "AllowDeployCustomerArtifactsWrite"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::305232526136:role/DeployCustomerArtifactsWrite"]
    }
    resources = [
      "${local.readysettech_cfn_public_bucket_arn}",
      "${local.readysettech_cfn_public_objects_arn}"
    ]
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket"
    ]
  }

  statement {
    sid    = "AllowPublicRead"
    effect = "Allow"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = [
      "s3:GetObject", "s3:GetObjectVersion"
    ]
    resources = [
      "${local.readysettech_cfn_public_objects_arn}"
    ]
  }

  statement {
    sid     = "AllowTLSRequestsOnly"
    actions = ["s3:*"]
    effect  = "Deny"
    resources = [
      "${local.readysettech_cfn_public_bucket_arn}",
      "${local.readysettech_cfn_public_objects_arn}"
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

resource "aws_s3_bucket" "readysettech-cfn-public" {
  bucket = local.readysettech_cfn_public_bucket_name
  tags = {
    Name = local.readysettech_cfn_public_bucket_name
  }
  versioning {
    enabled = true
  }
}
resource "aws_s3_bucket_policy" "readysettech-cfn-public" {
  bucket = aws_s3_bucket.readysettech-cfn-public.bucket
  policy = data.aws_iam_policy_document.readysettech-cfn-public.json
}

resource "aws_s3_bucket_public_access_block" "readysettech-cfn-public" {
  bucket = aws_s3_bucket.readysettech-customer-artifacts.bucket

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = true
}
