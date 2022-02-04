#-------------- [ General ] ------------------------------------------- #

variable "aws_region" {
  description = "The AWS region to create resources in."
  type        = string
}

variable "resource_tags" {
  description = "Base AWS resource tags to apply to any resources."
  default     = {}
  type        = map(any)
}

#-------------- [ Cluster ] ------------------------------------------- #

variable "cluster_name" {
  description = "The name of the k8s cluster being deployed into."
  type        = string
}

variable "self_managed_node_groups" {
  description = "Self-managed node group configs outputted by EKS module."
  type        = any
}

variable "cluster_oidc_issuer_url" {
  description = "OIDC issuer URL for the EKS cluster."
  type        = string
}

#-------------- [ Helm ] ---------------------------------------------- #

variable "helm_deployment_name" {
  description = "Name of the Helm release to create."
  default     = "aws-node-termination-handler"
  type        = string
}

variable "helm_chart_repository" {
  description = "Helm chart repository that hosts the helm_chart_name."
  default     = "https://aws.github.io/eks-charts"
  type        = string
}

variable "helm_chart_name" {
  description = "Name of the Helm chart to install from helm_chart_repository."
  default     = "aws-node-termination-handler"
  type        = string
}

variable "helm_chart_version" {
  description = "Version of Helm Chart to deploy."
  default     = "0.16.0"
  type        = string
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "kube-system"
  type        = string
}
