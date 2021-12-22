# Publicly accessible buckets for the installer (known externally, currently, as
# the "Orchestrator")

locals {
  installer_bucket_name = "readysettech-orchestrator-us-east-2"
  domain_name           = "launch.readyset.io"
}

data "vercel_team" "readyset" {
  slug = "readyset"
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
  value   = trimsuffix(aws_cloudfront_distribution.installer.domain_name, ".")
}
