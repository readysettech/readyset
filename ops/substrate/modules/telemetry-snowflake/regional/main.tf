locals {
  // Are we on step 1 or 2 of provisioning?
  //
  // See README.md for more information
  provisioning_step_1 = var.snowflake_external_id == "" && var.snowflake_iam_arn == ""
}

data "aws_iam_policy_document" "snowflake_telemetry" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      "${var.s3_bucket_arn}/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      var.s3_bucket_arn,
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

resource "aws_iam_role" "snowflake_telemetry" {
  name = "snowflake-telemetry"

  assume_role_policy = (
    local.provisioning_step_1
    // Placeholder assume role policy for step 1
    ? data.aws_iam_policy_document.placeholder_assume_role_policy.json
    // Actual assume role policy for step 2
    : data.aws_iam_policy_document.snowflake_assume_role_policy.json
  )
}

resource "aws_iam_role_policy" "snowflake_telemetry_s3_bucket_access" {
  name   = "AllowTelemetryS3BucketAccess"
  role   = aws_iam_role.snowflake_telemetry.id
  policy = data.aws_iam_policy_document.snowflake_telemetry.json
}

resource "snowflake_storage_integration" "telemetry_s3" {
  name    = "TELEMETRY_S3"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider = "S3"
  storage_allowed_locations = [
    "s3://${var.s3_bucket_name}"
  ]
  storage_aws_role_arn = aws_iam_role.snowflake_telemetry.arn
}

data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [snowflake_storage_integration.telemetry_s3.storage_aws_external_id]
    }

    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration.telemetry_s3.storage_aws_iam_user_arn]
    }
  }
}

resource "snowflake_database" "telemetry" {
  name = "TELEMETRY"
}

resource "snowflake_stage" "telemetry_s3" {
  count = local.provisioning_step_1 ? 0 : 1

  name                = "TELEMETRY_S3"
  url                 = "s3://${var.s3_bucket_name}"
  database            = snowflake_database.telemetry.name
  schema              = "PUBLIC"
  storage_integration = snowflake_storage_integration.telemetry_s3.name
}

resource "snowflake_external_table" "telemetry" {
  count = local.provisioning_step_1 ? 0 : 1

  database          = snowflake_database.telemetry.name
  schema            = "PUBLIC"
  name              = "telemetry"
  file_format       = "type = json"
  location          = "@${snowflake_database.telemetry.name}.PUBLIC.${snowflake_stage.telemetry_s3[0].name}"
  auto_refresh      = true
  copy_grants       = false
  refresh_on_create = true

  column {
    name = "id"
    type = "text"
    as   = "metadata$filename"
  }

  column {
    name = "data"
    type = "variant"
    as   = "value"
  }
}
