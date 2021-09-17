variable "environment" {
  description = "Substrate environment"
  type        = string
}

variable "buildkite_queues" {
  type = list(string)
}
