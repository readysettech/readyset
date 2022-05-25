# partially managed by Substrate; do not edit the archive, aws, or external providers by hand

terraform {
  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.2.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.9.0"
      configuration_aliases = [
        aws.us-east-1,
      ]
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.1"
    }
  }
  required_version = "= 1.1.6"
}
