# Publicly accessible buckets for the installer (known externally, currently, as
# the "Orchestrator")

locals {
  installer_bucket_name        = "readysettech-orchestrator-us-east-2"
  installer_bucket_arn         = "arn:aws:s3:::${local.installer_bucket_name}"
  installer_bucket_objects_arn = "${local.installer_bucket_arn}/*"
  domain_name                  = "launch.readyset.io"
}

data "vercel_team" "readyset" {
  slug = "readyset"
}

data "aws_iam_policy_document" "readysettech-installer-policy" {
  statement {
    sid    = "PublicReadGetObject"
    effect = "Allow"
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [
      local.installer_bucket_arn,
      local.installer_bucket_objects_arn
    ]
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
  }
  statement {
    sid    = "AllowDeployInstallerWrite"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::888984949675:role/InstallerS3"]
    }
    resources = [
      local.installer_bucket_arn,
      local.installer_bucket_objects_arn
    ]
    actions = [
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:PutObject",
      "s3:PutObjectAcl",
      "s3:ListBucket"
    ]
  }
  statement {
    sid    = "AllowDeployInstallerRead"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::069491470376:root", "arn:aws:iam::716876017850:root"]
    }
    resources = [
      local.installer_bucket_arn,
      local.installer_bucket_objects_arn
    ]
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
  }
}

resource "aws_s3_bucket" "installer" {
  bucket = local.installer_bucket_name
  acl    = "public-read"
  tags = {
    Name = local.installer_bucket_name
  }
  versioning {
    enabled = true
  }
  website {
    index_document = "readyset-orchestrator.sh"
  }
}

resource "aws_s3_bucket_policy" "installer-bucket-policy" {
  bucket = aws_s3_bucket.installer.bucket
  policy = data.aws_iam_policy_document.readysettech-installer-policy.json
}

resource "aws_acm_certificate" "installer" {
  provider          = aws.global # only us-east-1 is supported for cloudfront certs
  domain_name       = local.domain_name
  validation_method = "DNS"
}

resource "vercel_dns" "installer_validation" {
  for_each = {
    for dvo in aws_acm_certificate.installer.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  domain  = "readyset.io"
  name    = trimsuffix(each.value.name, ".readyset.io.")
  type    = each.value.type
  value   = each.value.record
  team_id = data.vercel_team.readyset.id
}

resource "aws_acm_certificate_validation" "installer" {
  provider        = aws.global # only us-east-1 is supported for cloudfront certs
  certificate_arn = aws_acm_certificate.installer.arn
  validation_record_fqdns = [
    for record in vercel_dns.installer_validation : "${record.name}.readyset.io"
  ]
}

resource "aws_cloudfront_distribution" "installer" {
  origin {
    domain_name = aws_s3_bucket.installer.website_endpoint
    origin_id   = local.domain_name

    custom_origin_config {
      http_port              = "80"
      https_port             = "443"
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1", "TLSv1.1", "TLSv1.2"]
    }
  }

  enabled             = true
  default_root_object = "readyset-orchestrator.sh"

  default_cache_behavior {
    viewer_protocol_policy = "redirect-to-https"
    compress               = true
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = local.domain_name
    min_ttl                = 0
    default_ttl            = 86400
    max_ttl                = 31536000

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }
  }

  aliases = ["${local.domain_name}"]

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn = aws_acm_certificate_validation.installer.certificate_arn
    ssl_support_method  = "sni-only"
  }
}

resource "vercel_dns" "installer" {
  team_id = data.vercel_team.readyset.id
  domain  = "readyset.io"
  name    = "launch"
  type    = "CNAME"
  value   = "${aws_cloudfront_distribution.installer.domain_name}."
}
