# managed by Substrate; do not edit by hand

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::069491470376:role/Administrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager          = "Terraform"
      SubstrateVersion = "2021.07"
    }
  }
  region = "sa-east-1"
}

provider "aws" {
  alias = "network"
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
  region = "sa-east-1"
}
