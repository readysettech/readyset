#-------------- [ General ] ------------------------------------------------- #

variable "aws_region" {
  description = "The AWS region to create resources in."
  type        = string
}

variable "resource_tags" {
  description = "Base AWS resource tags to apply to any resources."
  default     = {}
  type        = map(any)
}

#-------------- [ Cluster Context ] ----------------------------------------- #

variable "cluster_name" {
  description = "The name of the k8s cluster being deployed into."
  type        = string
}

variable "oidc_issuer_url" {
  description = "OIDC issuer URL for the EKS cluster."
}

variable "oidc_provider_arn" {
  description = "The OIDC provider ARN for the EKS cluster."
  type        = string
}

#-------------- [ Helm ] ------------------------------------------------- #

variable "helm_deployment_name" {
  description = "Name of the Helm release to be created."
  default     = "aws-load-balancer-controller"
}

variable "helm_chart_repository" {
  description = "Helm chart repository that hosts the helm_chart_name."
  default     = "https://aws.github.io/eks-charts"
}

variable "helm_chart_name" {
  description = "Name of the chart in helm_chart_repository that's to be installed."
  default     = "aws-load-balancer-controller"
}

variable "helm_chart_version" {
  description = "Version of the Helm Chart to deploy."
  default     = "1.3.3"
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "kube-system"
}

variable "helm_service_account_name" {
  description = "Name of the k8s service account to bind the created IAM role to."
  default     = "aws-load-balancer-controller"
  type        = string
}
