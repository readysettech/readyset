
data "aws_region" "current" {}
data "aws_kms_key" "s3-default" {
  key_id = "alias/aws/s3"
}
