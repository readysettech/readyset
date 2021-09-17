# managed by Substrate; do not edit by hand

module "substrate" {
  providers = {
    aws         = aws
    aws.network = aws.network
  }
  source = "../../substrate/regional"
}
