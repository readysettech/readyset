#-------------- [ Helm ] ---------------------------------------------- #

variable "helm_deployment_name" {
  description = "Name of the Helm release to create."
  default     = "prom-stack"
  type        = string
}

variable "helm_chart_repository" {
  description = "Helm chart repository that hosts the helm_chart_name."
  default     = "https://prometheus-community.github.io/helm-charts"
  type        = string
}

variable "helm_chart_name" {
  description = "Name of the Helm chart to install from helm_chart_repository."
  default     = "kube-prometheus-stack"
  type        = string
}

variable "helm_chart_version" {
  description = "Version of Helm Chart to deploy."
  default     = "32.2.0"
  type        = string
}

variable "helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "build"
  type        = string
}

#-------------- [ Prometheus Stack Configs ] -------------------------- #

variable "prometheus_ebs_volume_size_gb" {
  description = "Volume size in GiB for PVs created for Prometheus. For eb2, disk space directly impacts I/O, which could degrade performance."
  default     = 500
  type        = number
}

variable "prometheus_ebs_volume_storage_class" {
  description = "Kubernetes storage class of PVs created for Prometheus. This typically points at a storage class backed by Amazon EBS CSI."
  default     = "gp2"
  type        = string
}

variable "grafana_password" {
  description = "Password for Grafana."
  type        = string
}

variable "prometheus_pod_resources" {
  description = "Pod resource definitions for Prometheus pods deployed via the Operator."
  default = {
    requests = {
      cpu    = "500m"
      memory = "1024Mi"
    }
  }
}

variable "prometheus_metric_retention_days" {
  description = "Retention period in days for Prometheus cluster metrics."
  default     = "180d"
  type        = string
}
