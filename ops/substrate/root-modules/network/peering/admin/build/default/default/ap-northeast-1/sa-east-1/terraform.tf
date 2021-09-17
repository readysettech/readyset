# managed by Substrate; do not edit by hand

terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-ap-northeast-1"
    dynamodb_table = "terraform-state-locks"
    key            = "root-modules/network/peering/admin/build/default/default/ap-northeast-1/sa-east-1/terraform.tfstate"
    region         = "ap-northeast-1"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
