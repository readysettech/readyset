terraform {
  backend "s3" {
    bucket         = "readysettech-terraform-state-us-east-2"
    dynamodb_table = "terraform-state-locks"
    key            = "terraform/managed-services/eks-clusters"
    region         = "us-east-2"
    role_arn       = "arn:aws:iam::888984949675:role/TerraformStateManager"
  }
}
