# partially managed by Substrate; do not edit the archive, aws, or external providers by hand

terraform {
  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.2.0"
    }
    aws = {
      configuration_aliases = [
        aws.network
      ]
      source = "hashicorp/aws"
    }
    external = {
      source = "hashicorp/external"
    }
  }
}
