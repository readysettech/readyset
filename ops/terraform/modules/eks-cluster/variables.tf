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

variable "environment" {
  description = "The name of the environment."
  type        = string
}

variable "kubernetes_namespaces" {
  description = "List of Kubernetes namespaces to be created by the module inside the EKS cluster."
  type        = list(string)
  default     = []
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
  default     = "1.21"
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

variable "cluster_vpn_cidr_blocks" {
  description = "Ingress IPv4 CIDR block where VPN traffic originates while hitting the k8s control plane."
  default     = []
  type        = list(string)
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
          instance_warmup        = 240,
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

variable "self_managed_node_group_defaults" {
  description = "Map of self-managed node group default configurations."
  type        = any
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

variable "external_dns_internal_zone_mode" {
  description = "Determines what DNS zone the internal deployment will use. Can be used with a public zone."
  default     = "private"
}

#-------------- [ AWS LB Controller ] --------------------------------- #

variable "aws_lb_controller_enabled" {
  description = "Toggles provisioning of the AWS-LB-Controller module's resources."
  default     = true
  type        = bool
}

#-------------- [ Ingress Configs ] ----------------------------------- #

variable "alb_internal_ingress_enabled" {
  description = "Toggles deployment of Internal load balancer ingress definitions."
  default     = true
  type        = bool
}

variable "alb_acm_cert_arn" {
  description = "ACM certificate arn to be applied to ingress' listeners."
  default     = ""
}

variable "ns_ingress_routing_rules" {
  description = "Map of maps representing per-namespace ingress hosts to be provisioned."
  type        = map(any)
}

#-------------- [ Helm Chart Configs ] -------------------------------- #

variable "chart_version_defaults" {
  description = "The default versions for Helm charts that will be deployed along side the Kubernetes cluster."
  default = {
    cluster_autoscaler = "9.12.0"
    node_term_handler  = "0.16.0"
    external_dns       = "6.1.4"
    aws_lb_controller  = "1.3.3"
    bench_prom_grafana = "32.2.0"
  }
  type = map(any)
}

variable "chart_versions" {
  description = "Chart version override map. Setting values here will override what's set in chart_version_defaults."
  default     = {}
  type        = map(any)
}

#-------------- [ Benchmarking Prom + Grafana ] ---------------------- #

variable "benchmark_prom_grafana_enabled" {
  description = "Toggles provisioning of the benchmarking initiative's Prometheus and Grafana related resources."
  default     = false
}

variable "benchmark_ssm_path_grafana_password" {
  description = "SSM path suffix to Grafana password. Will be prefixed with cluster name automatically."
  default     = "/benchmark/grafana/adminPassword"
}

#-------------- [ Image Pull Secret Refresher ] ---------------------- #

variable "ips_refresher_enabled" {
  description = "Toggles the deployment of the IPS-refresher Helm chart. This provides creds to k8s when pulling from private ECR repos."
  default     = true
}

variable "ips_refresher_authorized_ecr_resource_arns" {
  description = "IAM resource list of ECR ARNs to grant read-access to. Supports wildcarding."
  default     = ["*"]
}

variable "ips_refresher_ecr_aws_account_id" {
  description = "AWS account ID which ECR repositories exist. There is currently a 1:1 relationship between ECR account and IPS-refresher deployments."
  default     = "305232526136" # build account
}
