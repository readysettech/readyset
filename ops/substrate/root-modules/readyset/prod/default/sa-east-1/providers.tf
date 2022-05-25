# managed by Substrate; do not edit by hand

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::431238456211:role/Administrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager = "Terraform"
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
      Manager = "Terraform"
    }
  }
  region = "sa-east-1"
}
