# partially managed by Substrate; do not edit the archive, aws, or external providers by hand

terraform {
  required_providers {
    archive = {
      source = "hashicorp/archive"
      version = ">= 2.2.0"
    }
    aws = {
      source = "hashicorp/aws"
      version = ">= 3.49.0"
    }
    external = {
      source = "hashicorp/external"
      version = ">= 2.1.0"
    }
  }
  required_version = "= 1.0.2"
}
