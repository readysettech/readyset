#-------------- [ Cluster ] ------------------------------------------- #

variable "cluster_name" {
  description = "Name of the EKS cluster to provision tags for."
  type        = string
  validation {
    condition = (
      length(var.cluster_name) > 1 && length(var.cluster_name) < 100 &&
      can(regex("^[0-9A-Za-z][A-Za-z0-9-_]*", var.cluster_name))
    )
    error_message = "EKS cluster name should be between 1-100 chars."
  }
}

#-------------- [ Networking ] ---------------------------------------- #

variable "vpc_id" {
  description = "ID of the VPC to deploy resources within."
  type        = string
}

variable "create_subnet_elb_tags" {
  description = "Toggles creation of necessary subnet tags which underpin auto-subnet discovery of LB Ingress Controller."
  default     = true
  type        = bool
}

#-------------- [ Data Source Params  ] ------------------------------- #

variable "vpc_subnet_data_source_key" {
  description = "Data source key to query VPC in order to locate various subnets."
  default     = "tag:Connectivity"
  type        = string
}

variable "vpc_public_subnet_data_value" {
  description = "Data source values to query VPC in order to locate public subnets."
  default     = ["public"]
  type        = list(string)
}

variable "vpc_private_subnet_data_value" {
  description = "Data source values to query VPC in order to locate private subnets."
  default     = ["private"]
  type        = list(string)
}
