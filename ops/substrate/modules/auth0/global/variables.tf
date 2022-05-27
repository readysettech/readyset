variable "auth0_domain" {
  type        = string
  description = "Auth0 admin API app domain."
}
variable "auth0_client_id" {
  type        = string
  description = "Auth0 admin API app client ID."
}
variable "auth0_client_secret" {
  type        = string
  description = "Auth0 admin API app client secret."
  sensitive   = true
}

variable "auth0_admin_user_password" {
  type        = string
  description = "Readyset application admin user password."
  sensitive   = true
}

variable "auth0_auditor_user_password" {
  type        = string
  description = "Readyset application auditor user password."
  sensitive   = true
}

variable "auth0_api_identifier" {
  type        = string
  description = "Unique identifier for the resource server. Used as the audience parameter for authorization calls."
  default     = "https://launch.readyset.io"
}

variable "readyset_logo_uri" {
  type        = string
  description = "URL of Readyset logo to use for login workflow brand. Should be about 150x150 pixels."
  default     = "https://readyset.io/favicon.png"
}

variable "callback_url" {
  type        = string
  description = "Callback URL, including protocol and port, for the API endpoint used for Auth0 callbacks."
  default     = "http://localhost:3000/callback"
}

variable "logout_url" {
  type        = string
  description = "Logout URL, including protocol and port, for the API endpoint used for Auth0 logouts."
  default     = "http://localhost:3000/"
}