# managed by Substrate; do not edit by hand

terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-us-west-2"
    dynamodb_table = "terraform-state-locks"
    key            = "root-modules/network/peering/admin/dev/default/default/us-west-2/us-west-2/terraform.tfstate"
    region         = "us-west-2"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
