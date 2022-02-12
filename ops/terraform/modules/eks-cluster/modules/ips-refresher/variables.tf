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

variable "ecr_account_id" {
  description = "The AWS account id that ECR repos are in."
  type        = string
}

variable "ips_projected_secret_name" {
  description = "The name of the k8s secret to project the credentials for accessing the registry specified by ecr_account_id/aws_region."
  default     = "readyset-ips"
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

#-------------- [ Helm ] ---------------------------------------------- #

variable "helm_deployment_name" {
  description = "Name of the Helm release to create."
  default     = "ips-refresher"
}

variable "helm_chart_name" {
  description = "Name of the Helm chart to install from helm_chart_repository."
  default     = "ips-refresher"
  type        = string
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "kube-system"
  type        = string
}

#-------------- [ IAM ] ----------------------------------------------- #

variable "authorized_ecr_resource_arns" {
  description = "IAM resource list of ECR ARNs to grant read-access to. Supports wildcarding."
  default     = ["*"]
  type        = list(string)
}

variable "k8s_service_account_name" {
  description = "Name of the k8s service account to add as an authorized subject during OIDC configuration."
  default     = "ips-refresher"
}
