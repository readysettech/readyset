terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      version               = ">= 3.72.0"
      configuration_aliases = [aws.dns]
    }
  }
}
