# managed by Substrate; do not edit by hand

terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-sa-east-1"
    dynamodb_table = "terraform-state-locks"
    key            = "root-modules/network/peering/admin/prod/default/default/sa-east-1/us-east-2/terraform.tfstate"
    region         = "sa-east-1"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
