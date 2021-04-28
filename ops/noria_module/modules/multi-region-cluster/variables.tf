variable "readyset_version" {
  type        = string
  description = "Readyset version to deploy (This is a required field, please ask for the latest version)."

  validation {
    condition     = can(regex("[0-9a-z]{7}", var.readyset_version)) && length(var.readyset_version) == 7
    error_message = "The readyset_version variable value needs to be an alphanumeric string of 7 characters (Without special symbols)."
  }
}

variable "key_name" {
  type        = string
  description = "Name of the EC2 key pair to use when provisioning instances."
}
