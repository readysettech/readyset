#-------------- [ General ] ------------------------------------------ #

variable "tailscale_ami_id" {
  description = "AMI ID to use when creating Tailscale Subnet Router EC2 instance(s)."
  type        = string
}

variable "environment" {
  description = "The name of the Substrate environment."
  type        = string
}

variable "quality" {
  description = "The name of the Substrate quality to label this deployment with."
  default     = "default"
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

#-------------- [ Tailscale ] ---------------------------------------- #

variable "tailscale_enabled" {
  description = "Toggles creation of Tailscale subnet router module's resources."
  default     = true
  type        = bool
}

variable "tailscale_instance_type" {
  description = "Instance type to apply to Tailscale subnet router EC2s."
  default     = "t3.micro"
  type        = string
}

variable "tailscale_root_volume_size" {
  description = "Size in GB for Root EBS volume of Tailscale subnet router instances."
  default     = 30
  type        = number
}

variable "tailscale_root_volume_del_on_term" {
  description = "Toggles deletion of root volume after Tailscale subnet router nodes are terminated."
  default     = true
  type        = bool
}

variable "tailscale_keypair_name" {
  description = "Name of the EC2 key pair to apply to the Tailscale subnet router instances."
  default     = "readyset-devops"
  type        = string
}

variable "tailscale_ssh_access_enabled" {
  description = "Toggles security group rule to allow inbound SSH traffic to Tailscale subnet router instances from ssh_allowed_cidrs"
  default     = false
  type        = bool
}

variable "tailscale_ssh_allowed_ingress_cidrs" {
  description = "List of CIDR blocks to authorize Tailscale subnet router ingress ssh traffic from."
  default     = ["10.0.0.0/8"]
  type        = list(string)
}

variable "tailscale_ssh_port_number" {
  description = "Port number that SSHd is listening on. Used to grant ingress rules on Tailscale security group."
  default     = 22
  type        = number
}

variable "tailscale_auth_key_secretsmanager_arn" {
  description = "ARN of Secrets Manager secret within this region to authorize Tailscale subnet router instance role access to."
  type        = string
}

variable "tailscale_secretsmanager_kms_key_arn" {
  description = "ARN of KMS key used for at-rest encryption of tailscale_auth_key_secretsmanager_arns."
  default     = ""
  type        = string
}
