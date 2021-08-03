# managed by Substrate; do not edit by hand

provider "aws" {
  alias = "accepter"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/NetworkAdministrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager          = "Terraform"
      SubstrateVersion = "2021.07"
    }
  }
  region = "us-west-2"
}

provider "aws" {
  alias = "requester"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/NetworkAdministrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager          = "Terraform"
      SubstrateVersion = "2021.07"
    }
  }
  region = "us-west-2"
}
