#-------------- [ General ] ------------------------------------------- #

variable "aws_region" {
  description = "The AWS region to create resources in."
  type        = string
}

#-------------- [ Route53 ] ------------------------------------------- #

variable "private_hosted_zone_name" {
  description = "When private_hosted_zone_enabled is true, this will be used to name the created private hosted zone."
  default     = "readyset.name"
  type        = string
}

variable "private_hosted_zone_enabled" {
  description = "Toggles provisioning of the readyset_domain_name private hosted zone in the Admin account."
  default     = false
}

variable "private_hosted_zone_id" {
  description = "If the private hosted zone already exists in the admin account, explicitly stating the zoneID here makes sense."
  default     = ""
}
