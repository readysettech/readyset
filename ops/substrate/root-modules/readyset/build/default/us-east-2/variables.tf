#-------------- [ General ] ------------------------------------------- #

variable "environment" {
  description = "The name of the Substrate environment."
  type        = string
}

variable "resource_tags" {
  description = "Base AWS resource tags to apply to resources."
  default = {
    managed = "terraform"
  }
  type = map(any)
}

#-------------- [ Build & Test Infra ] -------------------------------- #

variable "devops_assets_s3_bucket_enabled" {
  description = "Toggles creation of s3 bucket containing devops assets used during builds or benchmarking."
  default     = false
}

#-------------- [ Benchmarking ] -------------------------------------- #

variable "benchmarking_iam_role_enabled" {
  description = "Toggles creation of AWS IAM resources required for Benchmarking."
  type        = bool
  default     = false
}

variable "benchmarking_iam_role_trusted_account_ids" {
  description = "AWS accounts to permit assumption of Benchmarking IAM role. Should be empty if not using this role xaccount boundaries."
  type        = list(string)
  default     = []
}

variable "buildkite_k8s_queue_iam_role_enabled" {
  description = "Toggles creation of AWS IAM resources required for Benchmarking agents in the buildk8s queue."
  type        = bool
  default     = true
}

variable "buildkite_k8s_queue_iam_role_arns" {
  description = "ARN of additional IAM roles to grant Buildkite agents in the buildk8s queue permission to assume."
  default     = ["arn:aws:iam::305232526136:role/readyset-ci-k8s-build-us-east-2"]
  type        = list(string)
}
