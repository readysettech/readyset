# ----------- S3 Buckets - Devops Build Assets -------------------------------- #

resource "aws_s3_bucket" "devops-assets" {
  count  = var.devops_assets_s3_bucket_enabled ? 1 : 0
  acl    = "private"
  bucket = local.devops_assets_s3_bucket_name
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = data.aws_kms_key.s3-default.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }

  tags = merge(var.resource_tags, {
    Name = local.devops_assets_s3_bucket_name
    role = "builds"
  })
}

# Prevent all public and cross-account access
resource "aws_s3_bucket_public_access_block" "devops-assets" {
  count                   = var.devops_assets_s3_bucket_enabled ? 1 : 0
  bucket                  = aws_s3_bucket.devops-assets[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
