#-------------- [ General ] ------------------------------------------- #

variable "environment" {
  description = "The name of the Substrate environment."
  type        = string
}

variable "quality" {
  description = "The name of the Substrate quality to label this deployment with."
  type        = string
}

variable "resource_tags" {
  description = "Base AWS resource tags to apply to any resources."
  default     = {}
  type        = map(any)
}

variable "aws_region" {
  description = "The AWS region to create resources in."
  type        = string
}

variable "ami_owner_id" {
  description = "AWS account ID that owns the Tailscale subnet router AMI identified by ami_name_filter."
  default     = "716876017850"
  type        = string
}

variable "ami_name_filter" {
  description = "The name filter to apply when searching for Tailscale subnet router AMI. Ignored if ami_id is provided."
  default     = "readyset/images/hvm-ssd/tailscale-subnet-router-*"
  type        = string
}

#-------------- [ Systems ] ------------------------------------------- #

variable "ami_id" {
  description = "AMI to use for Tailscale subnet router EC2 instances. If blank, ami_name_filter logic will be used instead."
  default     = ""
  type        = string
}

variable "instance_type" {
  description = "Instance type to apply to Tailscale subnet router EC2s."
  type        = string
  default     = "t3.micro"
}

variable "root_volume_configs" {
  type        = object({ volume_size : number, delete_on_termination : bool })
  description = "Configuration object for root volumes associated with Tailscale subnet router EC2 instance."
}

variable "key_pair_name" {
  description = "The EC2 key pair to assign to the created Tailscale subnet router instance."
  type        = string
}

variable "enable_detailed_monitoring" {
  description = "Toggles CloudWatch detailed monitoring of Tailscale subnet router EC2 instance."
  default     = false
}

#-------------- [ Networking ] ---------------------------------------- #

variable "vpc_id" {
  description = "ID of the VPC to deploy Tailscale subnet routers within."
  type        = string
}

variable "ts_cfg_advertised_routes" {
  description = "CIDR ranges for Tailscale to advertise to SaaS control plane."
  default     = []
  type        = list(string)
}

variable "ssh_access_enabled" {
  default     = false
  description = "Toggles security group rule to allow inbound SSH traffic from ssh_allowed_cidrs"
  type        = bool
}

variable "ssh_allowed_ingress_cidrs" {
  description = "List of CIDR blocks to authorize ingress ssh traffic from."
  default     = ["10.0.0.0/18"]
  type        = list(string)
}

variable "ssh_port_number" {
  description = "Port number that SSHd is listening on. Used to grant ingress rules on Tailscale security group."
  default     = 22
  type        = number
}

#-------------- [ IAM ] ------------------------------------------------ #

variable "iam_authorized_secrets_manager_arn" {
  description = "Secrets Manager resource ARN to authorize Tailscale subnet router IAM role to read."
  type        = string
}

variable "iam_authorized_secrets_manager_kms_key_arn" {
  description = "ARN of the KMS key to be used when decrypting Secrets Manager secrets."
  type        = string
}
