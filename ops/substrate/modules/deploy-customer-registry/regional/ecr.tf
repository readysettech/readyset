locals {
  release_repositories = toset([
    "readyset-server",
    "readyset-mysql",
    "readyset-psql",
  ])
}

resource "aws_ecr_repository" "release_primary" {
  # We set up the primary if we are in the primary region and set up the repository replicas in others.
  for_each = data.aws_region.current.name == "us-east-2" ? local.release_repositories : toset([])
  name     = each.key
}

locals {
  iam_ids          = yamldecode(file("${path.module}/data/allowed_accounts.yaml"))
  iam_users        = local.iam_ids.users
  allowed_accounts = [for user_id in local.iam_users : "arn:aws:iam::${user_id}:root"]
}

data "aws_iam_policy_document" "release_primary_read_perms" {
  statement {
    sid = "ECRPublicRead"

    effect = "Allow"

    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetAuthorizationToken",
      "ecr:GetDownloadUrlForLayer",
      "ecr:GetRepositoryPolicy",
      "ecr:DescribeRepositories",
      "ecr:ListImages",
      "ecr:DescribeImages",
      "ecr:BatchGetImage",
    ]

    principals {
      identifiers = local.allowed_accounts
      type        = "AWS"
    }
  }
}

resource "aws_ecr_repository_policy" "release_primary_policy" {
  for_each   = aws_ecr_repository.release_primary
  repository = each.value.name
  policy     = data.aws_iam_policy_document.release_primary_read_perms.json
}