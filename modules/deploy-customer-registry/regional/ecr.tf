locals {
  customer_repositories = toset([
    "readyset-server",
    "readyset-mysql",
    "readyset-psql",
  ])

  customer_iam_arns = []

  primary_region_name = "us-east-2"
}

data "aws_region" "current" {}

# For the repositories we are mirroring, allow users in the Sandbox and Admin
# accounts to pull these repositories to allow ease of development.
data "aws_iam_policy_document" "customer" {
  version = "2008-10-17"
  # Allow those in the Readyset accounts to pull from this repository.
  statement {
    sid    = "AllowPullForReadysetInternal"
    effect = "Allow"
    principals {
      type = "AWS"
      identifiers = [
        "arn:aws:iam::069491470376:root",
        "arn:aws:iam::716876017850:root"
      ]
    }
    actions = [
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer"
    ]
  }
  # Allow those IAMs given from our customers to pull from these repositories.
  statement {
    sid    = "AllowPullForCustomer"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = local.customer_iam_arns
    }
    actions = [
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer"
    ]
  }
  # Allow ONLY this one IAM to push to these repositories
  statement {
    sid    = "AllowPushForBuildPushDeployECR"
    effect = "Allow"
    principals {
      type = "AWS"
      identifiers = [
        "arn:aws:iam::305232526136:role/PushDeployECR"
      ]
    }
    actions = [
      "ecr:BatchGetImage",
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:GetDownloadUrlForLayer",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart"
    ]
  }
}

resource "aws_ecr_repository" "customer_primary" {
  # We set up the primary if we are in the primary region and set up the repository replicas in others.
  for_each = data.aws_region.current.name == local.primary_region_name ? local.customer_repositories : toset([])
  name     = each.key
}

resource "aws_ecr_repository_policy" "customer_primary" {
  for_each   = aws_ecr_repository.customer_primary
  repository = each.value.name
  policy     = data.aws_iam_policy_document.customer.json
}
