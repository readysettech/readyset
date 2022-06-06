# partially managed by Substrate; do not edit the archive, aws, or external providers by hand

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.9.0"
    }
    external = {
      source  = "hashicorp/external"
      version = " ~> 2.1"
    }
    vercel = {
      source  = "registry.terraform.io/chronark/vercel"
      version = ">=0.10.3"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.2.0"
    }
  }
  required_version = "= 1.1.6"
}
