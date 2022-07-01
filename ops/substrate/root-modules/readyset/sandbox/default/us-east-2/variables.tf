# General
variable "environment" {
  description = "The name of the Substrate environment."
  type        = string
}
variable "quality" {
  description = "The name of the Substrate quality to label this deployment with."
  default     = "default"
  type        = string
}
variable "aws_region" {
  description = "The AWS region to create resources in."
  type        = string
}
# Auth0 Front End
variable "auth0_frontend_ami_id" {
  description = "ID of the auth0 frontend AMI to deploy"
  type        = string
}
variable "auth0_frontend_domain" {
  description = "DNS name at which to host the auth0 frontend application, not including .readyset.io"
  type        = string
  validation {
    condition     = substr(var.auth0_frontend_domain, -length(".readyset.io"), -1) != ".readyset.io"
    error_message = "The domain value must not end with .readyset.io."
  }
}
variable "auth0_frontend_instance_type" {
  description = "Instance type to use for the auth0 frontend instances"
  type        = string
  default     = "t2.micro"
}

variable "auth0_frontend_num_replicas" {
  description = "Number of replicas to deploy"
  type        = number
}
variable "auth0_frontend_key_name" {
  description = "Key name to use for the instances. To skip setting a key on the instances, set this variable to an empty string"
  type        = string
}
variable "auth0_client_id" {
  type        = string
  description = "Auth0 admin API app client ID."
}
variable "auth0_domain" {
  description = "Auth0 admin API app domain."
  type        = string
}
variable "auth0_audience" {
  description = "Auth0 API identifier; used as JWT audience parameter"
  type        = string
}
variable "auth0_frontend_issuer_base_url" {
  type        = string
  description = "The base url of the application."
}
variable "auth0_frontend_redirect_uri" {
  type        = string
  description = "The relative url path where Auth0 redirects back to."
}
variable "auth0_frontend_logout_uri" {
  type        = string
  description = "Where to redirect after logging out."
}
variable "auth0_rs_app_client_secret_arn" {
  type        = string
  description = "ARN for Auth0 Secret stored in secrets manager"
}