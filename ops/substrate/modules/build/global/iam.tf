locals {
  sccache_bucket_arn = "arn:aws:s3:::readysettech-build-sccache-us-east-2"
}

resource "aws_iam_user" "apple_build" {
  name = "AppleBuild"
}

data "aws_iam_policy_document" "apple_build_ip_restriction" {
  statement {
    effect = "Deny"
    actions = ["sts:AssumeRole"]
    principals {
      type = "AWS"
      identifiers = [aws_iam_user.apple_build.arn]
    }
    condition {
      test     = "NotIpAddress"
      variable = "aws:SourceIp"
      values   = ["63.135.169.95/32"]
    }
  }

    statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type = "AWS"
      identifiers = [aws_iam_user.apple_build.arn]
    }
  }

}

resource "aws_iam_role" "apple_build" {
  assume_role_policy   = data.aws_iam_policy_document.apple_build_ip_restriction.json
  max_session_duration = 43200
  name                 = "AppleBuildAccess"
}

resource "aws_iam_role_policy_attachment" "apple_connect_packer" {
  policy_arn = aws_iam_policy.packer_policy.arn
  role = aws_iam_role.apple_build.name
}

resource "aws_iam_role_policy_attachment" "apple_connect_cache" {
  policy_arn = aws_iam_policy.cache_buckets_policy.arn
  role = aws_iam_role.apple_build.name
}

resource "aws_iam_access_key" "apple-build-key" {
  user = aws_iam_user.apple_build.name
}

// Can't reference, so duplicating /ops/substrate/modules/buildkite-queue-shared/regional/iam.tf
resource "aws_iam_policy" "packer_policy" {
  name        = "AppleBuildPackerPolicy"
  description = "Allow running packer builds"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "ec2:AttachVolume",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:CopyImage",
          "ec2:CreateImage",
          "ec2:CreateKeypair",
          "ec2:CreateSecurityGroup",
          "ec2:CreateSnapshot",
          "ec2:CreateTags",
          "ec2:CreateVolume",
          "ec2:DeleteKeyPair",
          "ec2:DeleteSecurityGroup",
          "ec2:DeleteSnapshot",
          "ec2:DeleteVolume",
          "ec2:DeregisterImage",
          "ec2:DescribeImageAttribute",
          "ec2:DescribeImages",
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceStatus",
          "ec2:DescribeRegions",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSnapshots",
          "ec2:DescribeSubnets",
          "ec2:DescribeTags",
          "ec2:DescribeVolumes",
          "ec2:DetachVolume",
          "ec2:GetPasswordData",
          "ec2:ModifyImageAttribute",
          "ec2:ModifyInstanceAttribute",
          "ec2:ModifySnapshotAttribute",
          "ec2:RegisterImage",
          "ec2:RunInstances",
          "ec2:StopInstances",
          "ec2:TerminateInstances"
        ],
        Resource = "*"
      }
    ]
  })
}
data "aws_iam_policy_document" "cache_buckets_policy" {
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:Get*",
      "s3:List*"
    ]
    resources = [
          "${local.sccache_bucket_arn}/*",
          "${local.sccache_bucket_arn}"
      ]
  }
}

resource "aws_iam_policy" "cache_buckets_policy" {
  name        = "AppleBuildCacheBucketsPolicy"
  description = "Allow reads and writes to/from the cache buckets"
  policy = data.aws_iam_policy_document.cache_buckets_policy.json
}
