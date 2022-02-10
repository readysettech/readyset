provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::305232526136:role/Administrator"
    session_name = "Terraform"
  }
  # default_tags {
  #   tags = {
  #     Manager = "Terraform"
  #   }
  # }
  region = var.aws_region
}

provider "aws" {
  alias = "network"
  assume_role {
    role_arn     = "arn:aws:iam::911245771907:role/NetworkAdministrator"
    session_name = "Terraform"
  }
  # default_tags {
  #   tags = {
  #     Manager = "Terraform"
  #   }
  # }
  region = var.aws_region
}
