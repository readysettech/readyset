# managed by Substrate; do not edit by hand

module "intranet" {
  dns_domain_name = "readyset.name"
  providers = {
    aws           = aws
    aws.us-east-1 = aws.us-east-1
  }
  source = "../../../../modules/intranet/global"
}
