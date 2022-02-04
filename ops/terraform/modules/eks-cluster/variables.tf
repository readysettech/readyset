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
  description = "Name of EKS cluster to be created."
  type        = string
  validation {
    condition = (
      length(var.cluster_name) > 1 && length(var.cluster_name) < 100 &&
      can(regex("^[0-9A-Za-z][A-Za-z0-9-_]*", var.cluster_name))
    )
    error_message = "EKS cluster name should be between 1-100 chars."
  }
}

variable "cluster_version" {
  description = "Version of the EKS cluster."
  default     = "1.21.2"
  type        = string
}

variable "cluster_endpoint_private_only_access" {
  description = "Toggles private-only access mechanisms for the EKS cluster."
  default     = true
  type        = bool
}

variable "cluster_ip_family" {
  description = "The IP family used to assign Kubernetes pod and service addresses. Valid values are `ipv4` (default) and `ipv6`. You can only specify an IP family when you create a cluster, changing this value will force a new cluster to be created!"
  default     = "ipv4"
  type        = string
}

variable "cluster_service_ipv4_cidr" {
  description = "IPv4 CIDR block where k8s service will be provisioned from. This is a virtual network."
  default     = "10.100.0.0/16"
  type        = string
}

#-------------- [ Security / Auditing ]--------------------------------- #

variable "cluster_log_types" {
  description = "A list of log object types for k8s to log to CloudWatch."
  default = [
    "audit",
    "api",
    "authenticator"
  ]
  type = list(string)
}

variable "cluster_log_retention_days" {
  description = "Number of days for k8s CloudWatch logs to be retained."
  default     = 14
  type        = number
}

variable "kms_key_arn" {
  description = "ARN of KMS to be used for wrapper encryption of k8s secrets."
  type        = string
}

#-------------- [ Worker Nodes ] -------------------------------------- #

variable "self_managed_node_groups" {
  description = "Worker node configurations for the cluster."
  type        = map(any)
  default = {
    main = {
      max_size      = 2,
      desired_size  = 1,
      instance_type = "m5.large",
      instance_refresh = {
        strategy = "Rolling",
        preferences = {
          checkpoint_delay       = 600,
          checkpoint_percentages = [35, 70, 100],
          instance_warmup        = 300,
          min_healthy_percentage = 50,
        }
        triggers = ["tag"]
      }
      propogate_tags = [{
        key                 = "aws-node-termination-handler/managed"
        value               = true
        propagate_at_launch = true
      }]
    }
  }
}

#-------------- [ Networking ] ---------------------------------------- #

variable "vpc_id" {
  description = "ID of the VPC to deploy resources within."
  type        = string
}

variable "worker_subnet_ids" {
  description = "Subnet ids for placement of EKS worker nodes."
  type        = list(string)
}

variable "create_subnet_elb_tags" {
  description = "Toggles creation of necessary subnet tags which underpin auto-subnet discovery of LB Ingress Controller."
  default     = true
  type        = bool
}

variable "workers_ssh_cidr_allowed" {
  description = "CIDR ranges to allow SSH from. These should be non-publicly routable CIDR blocks."
  default     = []
}

#-------------- [ Cluster Autoscaler ] -------------------------------- #

variable "cluster_autoscaler_enabled" {
  description = "Toggles provisioning of the Cluster Autoscaler's related resources."
  default     = false
}

#-------------- [ Node Termination Handler ] -------------------------- #

variable "node_termination_handler_enabled" {
  description = "Toggles provisioning of the Node Termination Handler's related resources."
  default     = false
}

#-------------- [ External DNS ] -------------------------------------- #

variable "external_dns_internal_enabled" {
  description = "Toggles provisioning of the External-DNS (for private hosted zones) related resources."
  default     = false
  type        = bool
}

variable "external_dns_external_enabled" {
  description = "Toggles provisioning of the External-DNS (for public hosted zones) related resources."
  default     = false
  type        = bool
}

variable "external_dns_pub_zone_domain" {
  description = "Public hosted zone domain to use for ExternalDNS public DNS records."
  default     = ""
  type        = string
}

variable "external_dns_private_zone_domain" {
  description = "Private hosted zone domain to use for ExternalDNS private DNS records."
  default     = ""
  type        = string
}

#-------------- [ Helm Chart Configs ] -------------------------------- #

variable "chart_version_defaults" {
  description = "The default versions for Helm charts that will be deployed along side the Kubernetes cluster."
  default = {
    cluster_autoscaler = "9.12.0"
    node_term_handler  = "0.16.0"
    external_dns       = "2.20.3"
  }
  type = map(any)
}

variable "chart_versions" {
  description = "Chart version override map. Setting values here will override what's set in chart_version_defaults."
  default     = {}
  type        = map(any)
}
