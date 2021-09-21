locals {
  instance_types = [
    "t3a.small",
    "c5a.4xlarge",
  ]
}

module "buildkite_queue_shared" {
  source           = "../../../../../modules/buildkite-queue-shared/regional"
  environment      = "build"
  buildkite_queues = [for it in local.instance_types : replace(it, ".", "-")]
}

module "buildkite_queue" {
  for_each = toset(local.instance_types)

  source      = "../../../../../modules/buildkite-queue/regional"
  environment = "build"

  buildkite_queue = replace(each.key, ".", "-")

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  secrets_bucket   = module.buildkite_queue_shared.secrets_bucket
  artifacts_bucket = module.buildkite_queue_shared.artifacts_bucket

  instance_type = each.key
  max_size      = 3
}
