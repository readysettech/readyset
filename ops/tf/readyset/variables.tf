# Required variables
variable "env" {
  description = "Evironment name for the ReadySet deployment (i.e. dev, stage, prod, demo)"
  type        = string
}

variable "db_password" {
  description = "Password for the database connection"
  type        = string
  sensitive   = true
}

variable "key_name" {
  description = "Name of the EC2 key pair to use when provisioning instances"
  type        = string
}

variable "readyset_version" {
  description = "ReadySet version to deploy (Please ask for the latest version)"
  type        = string
}

variable "vpc" {
  description = "Name of the VPC"
  type        = string
}

# Optional variables
variable "allow_ssh" {
  description = "Allow SSH connections from within the VPC"
  type        = bool
  default     = false
}

variable "create_rds" {
  description = "Create an RDS instance using the Provisioned IOPS SSD storage class."
  type        = bool
  default     = false
}
variable "db_host" {
  description = "Hostname of the database to replicate"
  type        = string
  default     = ""
}

variable "db_name" {
  description = "Name of the database to replicate"
  type        = string
  default     = ""
}

variable "db_port" {
  description = "Port of the database to replicate"
  type        = number
  default     = 3306
}

variable "db_user" {
  description = "User for the database connection"
  type        = string
  default     = "admin"
}

variable "deployment" {
  description = "Unique identifier for the name of the ReadySet deployment"
  type        = string
  default     = "readyset"
}

variable "encrypt_rds_volume" {
  description = "Encrypt the RDS instance volume"
  type        = bool
  default     = false
}

variable "encrypt_server_volume" {
  description = "Encrypt the EBS volume used to persist server base tables"
  type        = bool
  default     = false
}

variable "encrypt_zookeeper_volume" {
  description = "Encrypt the EBS volume used to persist Zookeeper state"
  type        = bool
  default     = false
}

variable "extra_security_groups" {
  description = "List of security groups to associate with all EC2 instances"
  type        = list(string)
  default     = []
}

variable "mysql_adapter_allowed_cidr_blocks" {
  description = "List of CIDR blocks that are allowed to connect to the MySQL adapter"
  type        = list(string)
  default     = []
}

variable "mysql_adapter_instance_count" {
  description = "Number of EC2 instances in the MySQL adapter cluster"
  type        = number
  default     = 1
}

variable "mysql_adapter_instance_type" {
  description = "EC2 instance type of the MySQL adapter"
  type        = string
  default     = "t3.small"
}

variable "peer_region" {
  description = "Peer region name"
  type        = string
  default     = ""
}

variable "peer_zookeeper_ips" {
  description = "List of peer region Zookeeper IPs"
  type        = list(string)
  default     = []
}

variable "private_subnet_ids" {
  description = "Override subnet tag use with explicit list of private subnets IDs"
  type        = list(string)
  default     = []
}

variable "private_subnet_tag" {
  description = "Subnet tag value for private subnets"
  type        = string
  default     = "private"
}

variable "rds_allowed_cidr_blocks" {
  description = "List of CIDR blocks that are allowed to connect to the RDS instance when create_rds = true"
  type        = list(string)
  default     = []
}

variable "rds_engine_version" {
  description = "MySQL engine version to use when create_rds = true"
  type        = string
  default     = "8.0"
}

variable "rds_instance_type" {
  description = "RDS instance type when create_rds = true"
  type        = string
  default     = "db.t3.micro"
}

variable "rds_instance_allocated_storage" {
  description = "Allocated RDS storage in gigabytes when create_rds = true"
  type        = number
  default     = 10
}

variable "rds_instance_iops" {
  description = "Number of IOPS per instance when create_rds = true. This must be between 0.5 * rds_instance_allocated_storage and 50 * rds_instance_allocated_storage."
  type        = number
  default     = 500
}

variable "rds_volume_kms_key_id" {
  description = "ARN of the KMS key ID used to encrypt the RDS instance volume (ignored if encrypt_rds = false)"
  type        = string
  default     = ""
}

variable "server_allowed_cidr_blocks" {
  description = "List of CIDR blocks that are allowed to connect to the server"
  type        = list(string)
  default     = []
}

variable "server_instance_count" {
  description = "Number of server instances in the cluster"
  type        = number
  default     = 1
}

variable "server_instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.small"
}

variable "server_memory_bytes" {
  description = "Amount of memory, in bytes, for partially materialized state (0 = unlimited)"
  type        = number
  default     = 0
}

variable "server_quorum" {
  description = "Number of workers required to start"
  type        = number
  default     = 1
}

variable "server_shards" {
  description = "Number of shards to use (0 = disable sharding)"
  type        = number
  default     = 0
}

variable "server_volume_kms_key_id" {
  description = "ARN of the KMS key ID used to encrypt the server volume (ignored if encrypt_server_volume = false)"
  type        = string
  default     = ""
}

variable "server_volume_size" {
  description = "Size of the volume (in Gigabytes) for persisting base tables"
  type        = number
  default     = 100
}

variable "ssh_allowed_cidr_blocks" {
  description = "List of CIDR blocks that are allowed to conntect to instances via SSH (overrides allow_ssh)"
  type        = list(string)
  default     = []
}

variable "subnet_tag" {
  description = "Tag name used on subnets to define tier of connectivity"
  type        = string
  default     = "Connectivity"
}

variable "tags" {
  description = "Additional tags for resources"
  type        = map(any)
  default     = {}
}

variable "zookeeper_allowed_cidr_blocks" {
  description = "List of CIDR blocks that are allowed to connect to Zookeeper"
  type        = list(string)
  default     = []
}

variable "zookeeper_instance_count" {
  description = "Number of Zookeeper instances in the cluster (odd number recommended to maintain quorum)"
  type        = number
  default     = 1
}

variable "zookeeper_instance_type" {
  description = "EC2 instance type to use for the Zookeeper instance(s)"
  type        = string
  default     = "t3.small"
}

variable "zookeeper_volume_kms_key_id" {
  description = "ARN of the KMS key ID used to encrypt the Zookeeper volume (ignored if encrypt_zookeeper_volume = false)"
  type        = string
  default     = ""
}

variable "zookeeper_volume_size" {
  description = "Size of the volume (in Gigabytes) for persisting Zookeeper state"
  type        = number
  default     = 100
}
