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
    snowflake = {
      source  = "snowflake-labs/snowflake"
      version = "~> 0.34.0"
    }
  }
  required_version = "= 1.1.6"
}
