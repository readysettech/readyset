# managed by Substrate; do not edit by hand

resource "aws_s3_bucket" "readysettech-deploy-artifacts-sa-east-1" {
  bucket = "readysettech-deploy-artifacts-sa-east-1"
  tags = {
    Name = "readysettech-deploy-artifacts-sa-east-1"
  }
}
resource "aws_s3_bucket_policy" "readysettech-deploy-artifacts-sa-east-1" {
  bucket = aws_s3_bucket.readysettech-deploy-artifacts-sa-east-1.bucket
  policy = <<EOF
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": [
					"888984949675"
				]
			},
			"Action": [
				"s3:*"
			],
			"Resource": [
				"arn:aws:s3:::readysettech-deploy-artifacts-sa-east-1",
				"arn:aws:s3:::readysettech-deploy-artifacts-sa-east-1/*"
			]
		},
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": [
					"*"
				]
			},
			"Action": [
				"s3:GetObject",
				"s3:ListBucket"
			],
			"Resource": [
				"arn:aws:s3:::readysettech-deploy-artifacts-sa-east-1",
				"arn:aws:s3:::readysettech-deploy-artifacts-sa-east-1/*"
			],
			"Condition": {
				"StringEquals": {
					"aws:PrincipalOrgID": "o-09sxh7buzt"
				}
			}
		},
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": [
					"*"
				]
			},
			"Action": [
				"s3:PutObject",
				"s3:PutObjectAcl"
			],
			"Resource": [
				"arn:aws:s3:::readysettech-deploy-artifacts-sa-east-1/*"
			],
			"Condition": {
				"StringEquals": {
					"aws:PrincipalOrgID": "o-09sxh7buzt",
					"s3:x-amz-acl": "bucket-owner-full-control"
				}
			}
		}
	]
}
EOF
}
resource "aws_s3_bucket_public_access_block" "readysettech-deploy-artifacts-sa-east-1" {
  block_public_acls       = true
  block_public_policy     = true
  bucket                  = aws_s3_bucket.readysettech-deploy-artifacts-sa-east-1.bucket
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_versioning" "readysettech-deploy-artifacts-sa-east-1" {
  bucket = aws_s3_bucket.readysettech-deploy-artifacts-sa-east-1.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_ownership_controls" "readysettech-deploy-artifacts-sa-east-1" {
  bucket = aws_s3_bucket.readysettech-deploy-artifacts-sa-east-1.bucket
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}
