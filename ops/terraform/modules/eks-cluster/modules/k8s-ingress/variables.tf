#-------------- [ General ] ------------------------------------------- #

variable "aws_region" {
  description = "The AWS region to create resources in."
  type        = string
}

variable "environment" {
  description = "The name of the environment."
  type        = string
}

#-------------- [ Security / Network ] -------------------------------- #

variable "acm_cert_arn" {
  description = "ACM certificate arn to be applied to ingress' listeners."
  type        = string
}

variable "ingress_lb_class" {
  description = "Class of load balancer to annotate ingress resource with."
  default     = "alb"
}

#-------------- [ K8s Cluster ] --------------------------------------- #

variable "cluster_name" {
  description = "The cluster ID for the EKS cluster."
  type        = string
}

#-------------- [ Helm ] ---------------------------------------------- #

variable "helm_deployment_name" {
  description = "Name of the Helm release to create."
  default     = "k8s-ingress"
}

variable "helm_chart_name" {
  description = "Name of the Helm chart to install from helm_chart_repository."
  default     = "k8s-ingress"
  type        = string
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "kube-system"
  type        = string
}

variable "ns_ingress_routing_rules" {
  description = "Map of maps representing ingress hosts to be provisioned for the namespace."
  type        = any
}
