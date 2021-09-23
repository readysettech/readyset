# managed by Substrate; do not edit by hand

variable "dns_domain_name" {
  type = string
}

variable "oauth_oidc_client_id" {
  type = string
}

variable "oauth_oidc_client_secret_timestamp" {
  type = string
}

variable "okta_hostname" {
  default = ""
  type    = string
}

variable "selected_regions" {
  type = list(string)
}

variable "stage_name" {
  type = string
}
