# managed by Substrate; do not edit by hand

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::716876017850:role/Administrator"
    session_name = "Terraform"
  }
  region = "us-east-2"
}

provider "aws" {
  alias = "network"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/Auditor"
    session_name = "Terraform"
  }
  region = "us-east-2"
}
