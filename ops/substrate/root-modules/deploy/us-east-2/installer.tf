# Publicly accessible buckets for the installer (known externally, currently, as
# the "Orchestrator")

locals {
  installer_bucket_name = "readysettech-orchestrator-us-east-2"
}

resource "aws_s3_bucket" "installer" {
  bucket = local.installer_bucket_name
  acl    = "public-read"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Sid       = "PublicReadGetObject"
      Effect    = "Allow"
      Principal = "*"
      Action = [
        "s3:GetObject"
      ],
      Resource = [
        "arn:aws:s3:::${local.installer_bucket_name}/*"
      ]
    }]
  })

  website {
    index_document = "readyset-orchestrator.sh"
  }
}
