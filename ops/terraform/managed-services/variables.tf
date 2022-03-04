#-------------- [ General ] ------------------------------------------------- #

variable "resource_tags" {
  description = "Base AWS resource tags to apply to any resources."
  default     = {}
  type        = map(any)
}

variable "aws_region" {
  description = "The AWS region to create resources in."
  type        = string
}

variable "environment" {
  description = "The name of the environment."
  type        = string
}

variable "kubernetes_namespaces" {
  description = "List of Kubernetes namespaces for resources to be deployed within."
  type        = list(string)
  default     = []
}

#-------------- [ Networking ] ---------------------------------------- #

variable "vpc_id" {
  description = "ID of the VPC to deploy resources within."
  type        = string
}

#-------------- [ EKS Cluster ] --------------------------------------- #

variable "eks_cluster_name" {
  description = "Name of EKS cluster resources will be deployed within."
  type        = string
}
