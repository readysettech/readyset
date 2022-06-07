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

provider "snowflake" {
  username = "TERRAFORM"
  account  = "RA72744"
  region   = "us-east-2.aws"
  role     = "ACCOUNTADMIN"
}
