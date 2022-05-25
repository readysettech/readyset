# managed by Substrate; do not edit by hand

terraform {
  required_providers {
    archive = {
      source  = "hashicorp/archive"
    }
    aws = {
      source  = "hashicorp/aws"
      configuration_aliases = [
        aws.us-east-1,
      ]
    }
    external = {
      source  = "hashicorp/external"
    }
  }
}
