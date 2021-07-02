# managed by Substrate; do not edit by hand

terraform {
  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.2.0"
    }
    aws = {
      configuration_aliases = [
        aws.accepter,
        aws.requester,
      ]
      source  = "hashicorp/aws"
      version = ">= 3.45.0"
    }
    external = {
      source  = "hashicorp/external"
      version = ">= 2.1.0"
    }
  }
  required_version = "= 0.15.5"
}
