variable "production" {
  type        = bool
  description = "Only set this to true when wanting to attempt to build production images, ensure that the appropriate environment variables have been set"
  default     = env("PACKER_PRODUCTION") == "true"
}

variable "create_ami" {
  type        = bool
  description = "To save AMIs"
  default     = env("PACKER_CREATE_AMI") == "true"
}

variable "buildkite_commit" {
  type        = string
  description = "Used in development only for separating developers"
  default     = env("BUILDKITE_COMMIT")
}
