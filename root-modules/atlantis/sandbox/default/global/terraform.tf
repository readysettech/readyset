# managed by Substrate; do not edit by hand

terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-us-east-1"
    dynamodb_table = "terraform-state-locks"
    key            = "root-modules/atlantis/sandbox/default/global/terraform.tfstate"
    region         = "us-east-1"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
