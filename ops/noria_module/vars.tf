variable "vpc_id" {
  type        = string
  description = "ID of the VPC to deploy all resources into"
}

variable "deployment" {
  type        = string
  default     = "noria"
  description = "Unique identifier for the name of the Noria deployment"
}

variable "noria_version" {
  type        = string
  default     = "2ec92b13"
  description = "Version of Noria to deploy"
}

variable "zookeeper_instance_type" {
  type        = string
  default     = "m5.large"
  description = "EC2 instance type to use for the Zookeeper instance(s)"
}

variable "noria_server_instance_type" {
  type        = string
  default     = "m5.2xlarge"
  description = "EC2 instance type to use for the Noria server instance(s)"
}

variable "noria_mysql_instance_type" {
  type        = string
  default     = "m5.2xlarge"
  description = "EC2 instance type to use for the Noria MySQL adapter instance"
}

variable "noria_memory_bytes" {
  type        = number
  default     = 0
  description = "Amount of memory, in bytes, to provide to Noria for partially materialized state (0 = unlimited)"
}

variable "noria_quorum" {
  type        = number
  default     = 1
  description = "Number of noria workers to wait for before starting"
}

variable "noria_shards" {
  type        = number
  default     = 0
  description = "Number of shards to use in Noria (0 = disable sharding)"
}

variable "noria_disk_size_gb" {
  type        = number
  default     = 200
  description = "Size of the disk, in gigabytes, to provision for persisting Noria base tables"
}

variable "encrypt_noria_disk" {
  type        = bool
  default     = false
  description = "Encrypt the EBS volume used to persist Noria base tables"
}

variable "noria_disk_kms_key_id" {
  type        = string
  default     = ""
  description = "ARN for the KMS key ID to use to encrypt the noria volume. Ignored if encrypt_noria_disk = false."
}

variable "zookeeper_disk_size_gb" {
  type        = number
  default     = 200
  description = "Size of the disk, in gigabytes, to provision for persisting Zookeeper state"
}

variable "encrypt_zookeeper_disk" {
  type        = bool
  default     = false
  description = "Encrypt the EBS volume used to persist Zookeeper state"
}

variable "zookeeper_disk_kms_key_id" {
  type        = string
  default     = ""
  description = "ARN for the KMS key ID to use to encrypt the zookeeper volume. Ignored if encrypt_zookeeper_disk = false."
}

variable "key_name" {
  type        = string
  description = "Name of the EC2 key pair to use when provisioning instances"
}

variable "extra_security_groups" {
  type        = list(string)
  default     = []
  description = "List of extra security groups to associate with all provisioned EC2 instances"
}

variable "mysql_allowed_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "List of CIDR blocks that are allowed to connect to MySQL"
}

variable "associate_public_ip_addresses" {
  type        = bool
  default     = true
  description = "Whether or not to associate a public IP address with all provisioned instances"
}
