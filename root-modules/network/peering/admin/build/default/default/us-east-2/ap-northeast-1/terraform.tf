# managed by Substrate; do not edit by hand

terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-us-east-2"
    dynamodb_table = "terraform-state-locks"
    key            = "root-modules/network/peering/admin/build/default/default/us-east-2/ap-northeast-1/terraform.tfstate"
    region         = "us-east-2"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
