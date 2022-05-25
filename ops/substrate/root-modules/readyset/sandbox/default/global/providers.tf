# managed by Substrate; do not edit by hand

provider "aws" {
  alias = "us-east-1"
  assume_role {
    role_arn     = "arn:aws:iam::069491470376:role/Administrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager = "Terraform"
    }
  }
  region = "us-east-1"
}

provider "aws" {
  assume_role {
    role_arn     = "arn:aws:iam::069491470376:role/Administrator"
    session_name = "Terraform"
  }
  default_tags {
    tags = {
      Manager = "Terraform"
    }
  }
  region = "us-west-2"
}
