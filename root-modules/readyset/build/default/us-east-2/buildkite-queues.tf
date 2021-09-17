module "buildkite_queue_shared" {
  source           = "../../../../../modules/buildkite-queue-shared/regional"
  environment      = "build"
  buildkite_queues = ["c5a_4xlarge"]
}

module "buildkite_queue_c5a_4xlarge" {
  source      = "../../../../../modules/buildkite-queue/regional"
  environment = "build"

  buildkite_queue = "c5a_4xlarge"

  buildkite_agent_token_parameter_store_path = module.buildkite_queue_shared.buildkite_agent_token_parameter_store_path

  secrets_bucket   = module.buildkite_queue_shared.secrets_bucket
  artifacts_bucket = module.buildkite_queue_shared.artifacts_bucket

  instance_type = "c5a.4xlarge"
  max_size      = 3
}
