# managed by Substrate; do not edit by hand

provider "aws" {
  alias = "requester"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/NetworkAdministrator"
    session_name = "Terraform"
  }
  region = "ap-northeast-1"
}

provider "aws" {
  alias = "accepter"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/NetworkAdministrator"
    session_name = "Terraform"
  }
  region = "us-east-2"
}
