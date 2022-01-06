variable "environment" {
  description = "Substrate environment"
  type        = string
}

variable "secrets_bucket" {
  description = "Name of an existing S3 bucket containing pipeline secret."
  type        = string
}

variable "artifacts_bucket" {
  description = "Name of an existing S3 bucket for build artifact storage."
  type        = string
}

variable "buildkite_queue" {
  description = "Queue name that agents will use, targeted in pipeline steps using 'queue={value}'"
  type        = string
  default     = "default"
  validation {
    condition     = can(regex("^[a-zA-Z][-a-zA-Z0-9]+$", var.buildkite_queue))
    error_message = "Queue name is used for stack name so must be a valid stack name."
  }
}

variable "buildkite_agent_token_parameter_store_path" {
  description = "AWS SSM path to the Buildkite agent registration token. Expects a leading slash ('/')."
  type        = string
  validation {
    condition     = can(regex("^$|^/[a-zA-Z0-9_.\\-/]+$", var.buildkite_agent_token_parameter_store_path))
    error_message = "Expects a leading forward slash."
  }
}
variable "instance_type" {
  description = "Instance type. Comma-separated list with 1-4 instance types. The order is a prioritized preference for launching OnDemand instances, and a non-prioritized list of types to consider for Spot Instances (where used)."
  type        = string
  default     = "t3.large"
  validation {
    condition     = can(regex("^[\\w\\.]+(,[\\w\\.]*){0,3}$", var.instance_type))
    error_message = "Must contain 1-4 instance types separated by commas. No space before/after the comma."
  }
}

variable "max_size" {
  type        = number
  description = "Maximum number of instances"
  default     = 10
  validation {
    condition     = var.max_size > 0
    error_message = "Maximum number of instances must be at least one."
  }
}

variable "min_size" {
  type        = number
  description = "Maximum number of instances"
  default     = 0
}

variable "extra_iam_policy_arns" {
  type        = set(string)
  description = "List of ARNs for extra IAM policies to grant to the Role for the agent instances"
  default     = []
}

variable "ssh_key_pair_name" {
  type        = string
  description = "Name of the EC2 key pair to be applied to the Buildkite agents."
  default     = ""
}

variable "use_private_subnets" {
  type        = bool
  description = "Toggles the usage of private subnets within the target VPC, vs public."
  default     = false
}

variable "agent_additional_sudo_permissions" {
  type        = list(string)
  description = "List of extra sudo permissions to have granted to the Buildkite agents."
  default     = []
}
