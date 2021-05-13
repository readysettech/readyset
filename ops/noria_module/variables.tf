#---------------------------------------------------------------------------------------------------------------------#
# Required variables
#---------------------------------------------------------------------------------------------------------------------#

variable "aws_region" {
  type        = string
  description = "AWS Region where to deploy resources (e.g us-west-2)."

  validation {
    condition     = can(regex("(us(-gov)?|ap|ca|cn|eu|sa)-(central|(north|south)?(east|west)?)-\\d", var.aws_region))
    error_message = "The aws_region variable value is not a valid AWS Region name."
  }
}

variable "readyset_version" {
  type        = string
  description = "Readyset version to deploy (This is a required field, please ask for the latest version)."
}

variable "key_name" {
  type        = string
  description = "Name of the EC2 key pair to use when provisioning instances"
}

#---------------------------------------------------------------------------------------------------------------------#
# Optional variables
#---------------------------------------------------------------------------------------------------------------------#

variable "deployment" {
  type        = string
  default     = "noria"
  description = "Unique identifier for the name of the Noria deployment"
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

variable "enable_rds_connector" {
  type        = bool
  default     = false
  description = "Whether to enable streaming writes from an Amazon RDS instance to Noria"
}

variable "rds_instance_id" {
  type        = string
  default     = ""
  description = "ID of the RDS instance to stream writes from. Required if enable_rds_connector is true"
}

variable "db_host" {
  type        = string
  default     = ""
  description = "Hostname of the MySQL database to replicate to Noria"
}

variable "db_port" {
  type        = string
  default     = ""
  description = "Port of the MySQL database to replicate to Noria"
}

variable "db_name" {
  type        = string
  default     = ""
  description = "Name of the MySQL database to replicate to Noria"
}

variable "db_user" {
  type        = string
  default     = ""
  description = "MySQL user to use to connect to RDS"
}

variable "db_password" {
  type        = string
  default     = ""
  description = "Password for the MySQL user to use to connect to RDS"
}

variable "kafka_instance_type" {
  type        = string
  default     = "m5.xlarge"
  description = "EC2 instance type to use for the Kafka instance"
}

variable "debezium_instance_type" {
  type        = string
  default     = "t3.medium"
  description = "EC2 instance type to use for the Debezium instance"
}

variable "debezium_connector_instance_type" {
  type        = string
  default     = "t3.medium"
  description = "EC2 instance type to use for the Debezium Connector instance"
}

variable "tables" {
  type        = list(string)
  default     = []
  description = "List of tables to replicate from RDS"
}

variable "setup_id" {
  type        = string
  default     = ""
  description = "ID of the setup, 5 characters and no special symbols. (Useful for identifying resources)."

  validation {
    condition     = can(regex("[[:alnum:]]{5}", var.setup_id)) || length(var.setup_id) == 0
    error_message = "The setup_id variable value needs to be an alphanumeric string of 5 characters (Without special symbols)."
  }
}

variable "vpc_cidr_block" {
  type        = string
  default     = "10.0.0.0/16"
  description = "The CIDR block for the VPC."
}

variable "vpc_private_cidr_blocks" {
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  description = "A list of private CDIR blocks inside the VPC."
}

variable "vpc_public_cidr_blocks" {
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
  description = "A list of public CIDR blocks inside the VPC."
}

variable "allow_ssh" {
  type        = bool
  default     = false
  description = "Allow SSH connections from 0.0.0.0/0."
}

variable "readyset_server_allowed_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "List of CIDR blocks that are allowed to connect to Readyset server."
}

variable "zookeeper_allowed_cidr_blocks" {
  type        = list(string)
  default     = []
  description = "List of CIDR blocks that are allowed to connect to Zookeeper server."
}
