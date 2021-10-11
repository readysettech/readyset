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
    agents:
      queue: t3a-small
STEPS
}


resource "buildkite_pipeline" "readyset-nightly" {
  name           = "readyset-nightly"
  description    = "Slower, periodically scheduled builds for Readyset"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload .buildkite/pipeline.readyset-nightly.yml"
    label: ":pipeline:"
    agents:
      queue: t3a-small
STEPS
}

resource "buildkite_pipeline" "readyset-fuzz" {
  name           = "readyset-fuzz"
  description    = "Regular randomized fuzz testing using logictest"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload .buildkite/pipeline.readyset-fuzz.yml"
    label: ":pipeline:"
    agents:
      queue: t3a-small
STEPS
}

resource "buildkite_pipeline" "framework-testing" {
  name           = "framework-testing"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload .buildkite/pipeline.framework-testing.yml"
    label: ":pipeline:"
    agents:
      queue: t3a-small
STEPS
}

resource "buildkite_pipeline" "mirror-docker-hub-to-ecr" {
  name           = "mirror-docker-hub-to-ecr"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload .buildkite/pipeline.mirror-docker-hub-to-ecr.yml"
    label: ":pipeline:"
    agents:
      queue: t3a-small
STEPS
}

resource "buildkite_pipeline" "internal-amis" {
  name           = "internal-amis"
  default_branch = "refs/heads/main"
  repository     = local.repository_url

  steps = <<STEPS
steps:
  - command: "buildkite-agent pipeline upload .buildkite/pipeline.internal-amis.yml"
    label: ":pipeline:"
    agents:
      queue: t3a-small
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
    agents:
      queue: t3a-small
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
    agents:
      queue: t3a-small
STEPS
}
