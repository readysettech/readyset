terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 3.45.0"
    }
  }
  required_version = ">= 1.0.2"
}
