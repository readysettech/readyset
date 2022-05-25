# managed by Substrate; do not edit by hand

terraform {
  required_providers {
    archive = {
      source  = "hashicorp/archive"
    }
    aws = {
      configuration_aliases = [
        aws.network,
      ]
      source  = "hashicorp/aws"
    }
    external = {
      source  = "hashicorp/external"
    }
  }
}
