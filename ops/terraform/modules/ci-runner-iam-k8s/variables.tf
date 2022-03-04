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

variable "iam_role_trusted_account_ids" {
  description = "AWS accounts to permit assumption of the created IAM role. Should be empty if not using this role to traverse account boundaries."
  type        = list(string)
  default     = []
}

variable "k8s_role_name" {
  description = "Name of Kubernetes RBAC role to be created for mapping to the IAM role."
  default     = "ci-runner"
}

variable "k8s_role_grants" {
  description = "Object representing k8s RBAC rules to be applied to the k8s role."
  type        = map(any)
  default = {
    build = [{
      api_groups = [
        "apps",
        "batch",
        "extensions"
      ],
      resources = [
        "configmaps",
        "cronjobs",
        "jobs",
        "pods",
        "services"
      ],
      verbs = [
        "create",
        "describe",
        "get",
        "watch",
        "list",
        "delete"
      ]
    }]
  }
}
