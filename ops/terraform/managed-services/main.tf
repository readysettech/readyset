
module "ci-k8s-rbac" {
  source                = "../modules/ci-runner-iam-k8s"
  aws_region            = var.aws_region
  resource_tags         = var.resource_tags
  environment           = var.environment
}
