# managed by Substrate; do not edit by hand

module "substrate" {
  providers = {
    aws         = aws
    aws.network = aws
  }
  source = "../../substrate/regional"
}
