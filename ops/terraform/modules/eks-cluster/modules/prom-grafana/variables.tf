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

#-------------- [ Push GW Helm ] -------------------------------------- #

variable "pushgw_enabled" {
  description = "Toggles creation of Prometheus Push Gateway resources.."
  default     = true
  type        = bool
}

variable "prometheus_hostname" {
  description = "Hostname that has been assigned to Prometheus service outside of this module."
  default     = "prom-stack-kube-prometheus-prometheus:9090"
  type        = string
}

variable "pushgw_hostname" {
  description = "Hostname that has been assigned to Prometheus Push Gateway, outside of this module."
  default     = "prom-pushgw-prometheus-pushgateway:9091"
  type        = string
}

variable "pushgw_helm_deployment_name" {
  description = "Name of the Helm release to create."
  default     = "prom-pushgw"
  type        = string
}

variable "pushgw_helm_chart_repository" {
  description = "Helm chart repository that hosts the helm_chart_name."
  default     = "https://prometheus-community.github.io/helm-charts"
  type        = string
}

variable "pushgw_helm_chart_name" {
  description = "Name of the Helm chart to install from helm_chart_repository."
  default     = "prometheus-pushgateway"
  type        = string
}

variable "pushgw_helm_chart_version" {
  description = "Version of Helm Chart to deploy."
  default     = "1.15.0"
  type        = string
}

variable "pushgw_helm_deployment_namespace" {
  description = "Namespace to deploy the Helm chart into."
  default     = "build"
  type        = string
}

variable "pushgw_replica_count" {
  description = "Replica count for Push Gateway deployment in k8s."
  default     = 3
  type        = number
}

variable "pushgw_service_mon_namespace" {
  description = "Service monitor's namespace."
  default     = "build"
  type        = string
}

variable "pushgw_deployment_strategy" {
  description = "Deployment strategy value to be provided to Push Gateway chart."
  default     = "Recreate"
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
