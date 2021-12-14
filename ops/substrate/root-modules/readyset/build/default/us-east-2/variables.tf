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
