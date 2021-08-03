# managed by Substrate; do not edit by hand

resource "aws_s3_bucket" "readysettech-deploy-artifacts-us-east-2" {
  bucket = "readysettech-deploy-artifacts-us-east-2"
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
				"arn:aws:s3:::readysettech-deploy-artifacts-us-east-2",
				"arn:aws:s3:::readysettech-deploy-artifacts-us-east-2/*"
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
				"s3:ListBucket",
				"s3:PutObject"
			],
			"Resource": [
				"arn:aws:s3:::readysettech-deploy-artifacts-us-east-2",
				"arn:aws:s3:::readysettech-deploy-artifacts-us-east-2/*"
			],
			"Condition": {
				"StringEquals": {
					"aws:PrincipalOrgID": "o-09sxh7buzt"
				}
			}
		}
	]
}
EOF
  tags = {
    Name = "readysettech-deploy-artifacts-us-east-2"
  }
  versioning {
    enabled = true
  }
}
resource "aws_s3_bucket_public_access_block" "readysettech-deploy-artifacts-us-east-2" {
  block_public_acls       = true
  block_public_policy     = true
  bucket                  = aws_s3_bucket.readysettech-deploy-artifacts-us-east-2.bucket
  ignore_public_acls      = true
  restrict_public_buckets = true
}
