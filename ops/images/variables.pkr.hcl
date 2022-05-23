variable "production" {
  type        = bool
  description = "Set this to true when building production images. This makes sure that we are not overwriting production images."
  default     = env("PACKER_PRODUCTION") == "true"
}

variable "create_ami" {
  type        = bool
  description = "By default, we do not create an AMI permanently. Setting this will save the AMI into storage."
  default     = env("PACKER_CREATE_AMI") == "true"
}

variable "buildkite_commit" {
  type        = string
  description = "Git commit ID from Buildkite. Used to tag images made in Buildkite appropriately"
  default     = env("BUILDKITE_COMMIT")
}

variable "binaries_path" {
  type        = string
  description = "Path to statically built binaries to include in images"
  default     = env("BINARIES_PATH")
}
