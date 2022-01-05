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

variable "readyset_authority_consul_region_ami_id" {
  type        = string
  description = "Colon separated region and AMI ID for the readyset-authority-consul image"
  default     = env("READYSET_AUTHORITY_CONSUL_REGION_AMI_ID")
}

variable "readyset_bastion_region_ami_id" {
  type        = string
  description = "Colon separated region and AMI ID for the readyset-bastion image"
  default     = env("READYSET_BASTION_REGION_AMI_ID")
}

variable "readyset_monitor_region_ami_id" {
  type        = string
  description = "Colon separated region and AMI ID for the readyset-monitor image"
  default     = env("READYSET_MONITOR_REGION_AMI_ID")
}

variable "readyset_mysql_adapter_region_ami_id" {
  type        = string
  description = "Colon separated region and AMI ID for the readyset-mysql-adapter image"
  default     = env("READYSET_MYSQL_ADAPTER_REGION_AMI_ID")
}

variable "readyset_psql_adapter_region_ami_id" {
  type        = string
  description = "Colon separated region and AMI ID for the readyset-psql-adapter image"
  default     = env("READYSET_PSQL_ADAPTER_REGION_AMI_ID")
}

variable "readyset_server_region_ami_id" {
  type        = string
  description = "Colon separated region and AMI ID for the readyset-server image"
  default     = env("READYSET_SERVER_REGION_AMI_ID")
}
