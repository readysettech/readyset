# managed by Substrate; do not edit by hand

provider "aws" {
  alias = "global"
  assume_role {
    role_arn     = "arn:aws:iam::888984949675:role/DeployAdministrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager          = "Terraform"
      SubstrateVersion = "2021.09"
    }
  }
  region = "us-east-1"
}

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::888984949675:role/DeployAdministrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager          = "Terraform"
      SubstrateVersion = "2021.09"
    }
  }
  region = "us-west-2"
}

provider "aws" {
  alias = "network"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/Auditor"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager          = "Terraform"
      SubstrateVersion = "2021.09"
    }
  }
  region = "us-west-2"
}
