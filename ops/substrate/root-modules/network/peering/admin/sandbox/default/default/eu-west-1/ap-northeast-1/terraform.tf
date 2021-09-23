# managed by Substrate; do not edit by hand

terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-eu-west-1"
    dynamodb_table = "terraform-state-locks"
    key            = "root-modules/network/peering/admin/sandbox/default/default/eu-west-1/ap-northeast-1/terraform.tfstate"
    region         = "eu-west-1"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
