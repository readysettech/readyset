provider "buildkite" {
  organization = "readyset"
}

locals {
  # TODO get this IP from a terraformized gerrit instance
  repository_url = "ssh://buildkite@10.3.175.41:29418/readyset.git"
}

resource "buildkite_pipeline" "readyset" {
  name           = "readyset"
  description    = "Build pipeline for Readyset, run on every commit"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  skip_intermediate_builds                 = true
  cancel_intermediate_builds               = true
  cancel_intermediate_builds_branch_filter = "!refs/heads/main"

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload"
    label: ":pipeline:"
STEPS
}

resource "buildkite_pipeline" "docker-release" {
  name           = "docker-release"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload .buildkite/pipeline.docker-release.yml"
    label: ":pipeline:"
STEPS
}

resource "buildkite_pipeline" "external-amis" {
  name           = "external-amis"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload .buildkite/pipeline.external-amis.yml"
    label: ":pipeline:"
STEPS
}
