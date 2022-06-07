locals {
  // Are we on step 1 or 2 of provisioning?
  //
  // See README.md for more information
  provisioning_step_1 = var.snowflake_external_id == "" && var.snowflake_iam_arn == ""
}

// Suffix resources with a random ID to allow multiple s3 integrations per
// snowflake db
resource "random_id" "this" {
  byte_length = 8
}

data "aws_iam_policy_document" "snowflake_s3" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      for bucket in var.s3_buckets : "${bucket.arn}/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      for bucket in var.s3_buckets : bucket.arn
    ]
  }
}

data "aws_iam_policy_document" "placeholder_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    effect  = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "snowflake_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    effect  = "Allow"

    principals {
      type        = "AWS"
      identifiers = [var.snowflake_iam_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake" {
  name = "snowflake"

  assume_role_policy = (
    local.provisioning_step_1
    // Placeholder assume role policy for step 1
    ? data.aws_iam_policy_document.placeholder_assume_role_policy.json
    // Actual assume role policy for step 2
    : data.aws_iam_policy_document.snowflake_assume_role_policy.json
  )
}

resource "aws_iam_role_policy" "snowflake_s3_bucket_access" {
  name   = "AllowS3BucketAccess"
  role   = aws_iam_role.snowflake.id
  policy = data.aws_iam_policy_document.snowflake_s3.json
}

resource "snowflake_storage_integration" "aws_s3" {
  name    = "AWS_S3_${upper(random_id.this.hex)}"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider = "S3"
  storage_allowed_locations = [
    for bucket in var.s3_buckets : "s3://${bucket.bucket}"
  ]
  storage_aws_role_arn = aws_iam_role.snowflake.arn
}

// Unused, but provided to make it easier to copy-paste the required values for
// the 2-stage provisioning process
data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [snowflake_storage_integration.aws_s3.storage_aws_external_id]
    }

    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration.aws_s3.storage_aws_iam_user_arn]
    }
  }
}
