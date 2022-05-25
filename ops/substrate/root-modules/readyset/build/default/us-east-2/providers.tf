# managed by Substrate; do not edit by hand

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::305232526136:role/Administrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager = "Terraform"
    }
  }
  region = "us-east-2"
}

provider "aws" {
  alias = "network"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/NetworkAdministrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager = "Terraform"
    }
  }
  region = "us-east-2"
}
