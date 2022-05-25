# partially managed by Substrate; do not edit the archive, aws, or external providers by hand

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
    external = {
      source = "hashicorp/external"
    }
  }
}
