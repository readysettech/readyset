# managed by Substrate; do not edit by hand

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::716876017850:role/Administrator"
    session_name = "Terraform"
  }
  region = "us-east-1"
}
