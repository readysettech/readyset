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
  name     = "${each.key}"
}
