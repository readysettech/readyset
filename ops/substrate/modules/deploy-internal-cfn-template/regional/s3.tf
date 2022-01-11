locals {
  readysettech_cfn_internal_bucket_name = "readysettech-cfn-internal-${data.aws_region.current.name}"
  readysettech_cfn_internal_bucket_arn  = "arn:aws:s3:::${local.readysettech_cfn_internal_bucket_name}"
  readysettech_cfn_internal_objects_arn = "${local.readysettech_cfn_internal_bucket_arn}/*"
}

data "aws_region" "current" {}

data "aws_iam_policy_document" "readysettech-cfn-internal-bucket-policy" {
  statement {
    sid    = ""
    effect = "Allow"
    resources = [
      local.readysettech_cfn_internal_bucket_arn,
      local.readysettech_cfn_internal_objects_arn
    ]
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket"
    ]
    principals {
      # These resources should point to the Internal Artifacts Write IAM Role ARN
      # Note these values will need to be manually copied from the AWS console :c
      type        = "AWS"
      identifiers = ["arn:aws:iam::888984949675:role/InternalArtifactsWrite"]
    }

  }

  statement {
    sid    = ""
    effect = "Allow"
    resources = [
      local.readysettech_cfn_internal_bucket_arn,
      local.readysettech_cfn_internal_objects_arn
    ]
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    principals {
      # The principal with the ending of :root means anything in the account has
      # access if the IAM has access otherwise. This is documented at
      # https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html
      type = "AWS"
      identifiers = [
        "arn:aws:iam::069491470376:root", # Sandbox account
        "arn:aws:iam::305232526136:root", # Build account
      ]
    }
  }

  statement {
    sid     = "AllowTLSRequestsOnly"
    actions = ["s3:*"]
    effect  = "Deny"
    resources = [
      local.readysettech_cfn_internal_bucket_arn,
      local.readysettech_cfn_internal_objects_arn
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

resource "aws_s3_bucket" "readysettech-cfn-internal" {
  bucket = local.readysettech_cfn_internal_bucket_name
  tags = {
    Name = local.readysettech_cfn_internal_bucket_name
  }
  versioning {
    enabled = true
  }

  lifecycle_rule {
    id      = "monthly_retention"
    enabled = true

    expiration {
      days = 30
    }
  }

}
resource "aws_s3_bucket_policy" "readysettech-cfn-internal" {
  bucket = aws_s3_bucket.readysettech-cfn-internal.bucket
  policy = data.aws_iam_policy_document.readysettech-cfn-internal-bucket-policy.json
}

resource "aws_s3_bucket_public_access_block" "readysettech-cfn-internal" {
  bucket = aws_s3_bucket.readysettech-cfn-internal.bucket

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "readysettech_cfn_internal_bucket_arn" {
  value = local.readysettech_cfn_internal_bucket_arn
  #  description = "ARN of the Readyset Internal CFN Bucket in region ${data.aws_region.current.name}"
}

output "readysettech_cfn_internal_objects_arn" {
  value = local.readysettech_cfn_internal_objects_arn
  #  description = "ARN of the Readyset Internal CFN Bucket Object in region ${data.aws_region.current.name}"
}
