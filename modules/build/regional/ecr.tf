locals {
  mirror_repositories = toset([
    "rust",
    "rustlang/rust",

    "mysql",
    "postgres",
    "redis",

    "consul",
    "zookeeper",

    "node",
    "golang",
    "haskell",
    "openjdk",
    "drupal",
    "php",
    "python",
    "ruby",

    "debian",
    "alpine",
    "fedora",
  ])
}

data "aws_region" "current" {}

# For the repositories we are mirroring, allow users in the Sandbox and Admin
# accounts to pull these repositories to allow ease of development.
data "aws_iam_policy_document" "mirror" {
  version = "2008-10-17"
  statement {
    sid    = "AllowPull"
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
}

resource "aws_ecr_repository" "mirror_primary" {
  # We set up the primary if we are in the primary region and set up the repository replicas in others.
  for_each = data.aws_region.current.name == "us-east-2" ? local.mirror_repositories : toset([])
  name     = "mirror/${each.key}"
}

resource "aws_ecr_repository_policy" "mirror_primary" {
  for_each   = aws_ecr_repository.mirror_primary
  repository = each.value.name
  policy     = data.aws_iam_policy_document.mirror.json
}

// TODO: Import our existing repositories
