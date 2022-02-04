#-------------- [ Route53 ] ------------------------------------------- #

variable "r53_domain" {
  description = "The Route53 DNS domain ExternalDNS will be using."
  type        = string
}

variable "dns_zone_mode" {
  description = "The zone type for the module (public/private)."
  default     = "public"
  type        = string
}

#-------------- [ K8s Cluster ] --------------------------------------- #

variable "cluster_id" {
  description = "The cluster ID for the EKS cluster."
  type        = string
}

variable "oidc_provider_arn" {
  description = "The OIDC provider ARN for the EKS cluster."
  type        = string
}

variable "oidc_issuer_url" {
  description = "The OIDC issuer URL for the EKS cluster."
  type        = string
}

#-------------- [ Helm ] ---------------------------------------------- #

variable "helm_chart_repository" {
  description = "Helm chart repository that hosts the helm_chart_name."
  default     = "https://charts.bitnami.com/bitnami"
  type        = string
}

variable "helm_chart_name" {
  description = "Name of the Helm chart to install from helm_chart_repository."
  default     = "external-dns"
  type        = string
}

variable "helm_chart_version" {
  description = "Version of Helm Chart to deploy."
  default     = "2.20.3"
  type        = string
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "kube-system"
  type        = string
}
