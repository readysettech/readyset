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

variable "release_name" {
  type        = string
  description = "Name to give this release. Used to tag images."
  default     = env("RELEASE_NAME")
}

variable "readyset_authority_consul_ami_id" {
  type        = string
  description = "AMI ID for the readyset-authority-consul image"
  default     = env("READYSET_AUTHORITY_CONSUL_AMI_ID")
}

variable "readyset_mysql_adapter_ami_id" {
  type        = string
  description = "AMI ID for the readyset-mysql-adapter image"
  default     = env("READYSET_MYSQL_ADAPTER_AMI_ID")
}

variable "readyset_server_ami_id" {
  type        = string
  description = "AMI ID for the readyset-server image"
  default     = env("READYSET_SERVER_AMI_ID")
}
