variable "environment" {
  description = "Substrate environment"
  type        = string
}

variable "buildkite_queues" {
  type = list(string)
}

variable "agent_token_allowed_roles" {
  description = "List of IAM role ARNs that are allowed to access the agent token"
  type        = list(string)
}
