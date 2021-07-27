# managed by Substrate; do not edit by hand

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::305232526136:role/Administrator"
    session_name = "Terraform"
  }
  region = "us-east-1"
}
