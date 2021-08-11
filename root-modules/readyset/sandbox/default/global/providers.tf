# managed by Substrate; do not edit by hand

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::069491470376:role/Administrator"
    session_name = "Terraform"
  }
  region = "us-east-1"
}
