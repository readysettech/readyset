terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-us-west-2"
    dynamodb_table = "terraform-state-locks"
    key            = "root-modules/network/global/terraform.tfstate"
    region         = "us-west-2"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
