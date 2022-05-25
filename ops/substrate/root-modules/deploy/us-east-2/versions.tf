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
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.1"
    }
    vercel = {
      source  = "registry.terraform.io/chronark/vercel"
      version = ">=0.10.3"
    }
  }
  required_version = "= 1.1.6"
}
