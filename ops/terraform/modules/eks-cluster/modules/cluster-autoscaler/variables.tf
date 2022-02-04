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

variable "cluster_id" {
  description = "ID of the EKS cluster to be augmented with cluster autoscaling."
}

variable "cluster_oidc_issuer_url" {
  description = "OIDC issuer URL for the EKS cluster."
}

#-------------- [ Helm ] ------------------------------------------------- #

variable "helm_deployment_name" {
  description = "Name of the Cluster Autoscaler Helm release to create."
  default     = "cluster-autoscaler"
}

variable "helm_chart_repository" {
  description = "Helm chart repository that hosts the helm_chart_name."
  default     = "https://kubernetes.github.io/autoscaler"
}

variable "helm_chart_name" {
  description = "Name of the Cluster Autoscaler chart in helm_chart_repository."
  default     = "cluster-autoscaler"
}

variable "helm_chart_version" {
  description = "Version of Cluster Autoscaler Helm Chart to deploy."
  default     = "9.12.0"
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Cluster Autoscaler Helm chart into."
  default     = "kube-system"
}
