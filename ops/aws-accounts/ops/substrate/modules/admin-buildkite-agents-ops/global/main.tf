# This policy allows anyone in the organization to read the artifacts produced
# but only allow modifications of the bucket or the objects from the admin
# account.
data "aws_iam_policy_document" "readyset_admin_buildkite_ops_artifacts" {
  statement {
    principals {
      type = "AWS"
      identifiers = ["716876017850"]
    }
    actions = [ "s3:*"]
    resources = [
      "arn:aws:s3:::readysettech-admin-buildkite-ops-artifacts",
      "arn:aws:s3:::readysettech-admin-buildkite-ops-artifacts/*"
    ]
    effect = "Allow"
  }

  statement {
    principals {
      type = "AWS"
      identifiers = ["*"]
    }
    actions = [ "s3:GetObject", "s3:ListBucket" ]
    resources = [
      "arn:aws:s3:::readysettech-admin-buildkite-ops-artifacts",
      "arn:aws:s3:::readysettech-admin-buildkite-ops-artifacts/*"
    ]
    condition {
      test = "StringEquals"
      variable = "aws:PricipalOrgID"
      values = ["o-09sxh7buzt"]
    }
    effect = "Allow"
  }
}


# Creating a single global bucket in admin for Buildkite artifacts from an ops
# pipeline.
resource "aws_s3_bucket" "readysettech-admin-buildkite-ops-artifacts" {
  bucket = "readysettech-admin-buildkite-ops-artifacts"
  policy = data.aws_iam_policy_document.readyset_admin_buildkite_ops_artifacts.json
  tags = {
    Name = "readysettech-admin-buildkite-artifacts-ops"
  }
  versioning {
    enabled = true
  }
}

# Ensure on the AWS side that none of the artifacts ever become public.
resource "aws_s3_bucket_public_access_block" "readysettech-admin-buildkite-ops-artifacts" {
  block_public_acls       = true
  block_public_policy     = true
  bucket                  = aws_s3_bucket.readysettech-admin-buildkite-ops-artifacts.bucket
  ignore_public_acls      = true
  restrict_public_buckets = true
}
