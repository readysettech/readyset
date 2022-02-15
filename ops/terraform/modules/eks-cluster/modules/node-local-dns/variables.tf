#-------------- [ Helm ] ---------------------------------------------- #

variable "helm_deployment_name" {
  description = "Name of the Helm release to create."
  default     = "node-local-dns"
  type        = string
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "kube-system"
  type        = string
}

#-------------- [ Node-LocalDNS Configurations ] ---------------------- #

variable "vpc_dns_resolver_ip" {
  description = "Private IP of the VPC DNS resolver which is hosting the k8s cluster."
  default     = ""
}
