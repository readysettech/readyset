# managed by Substrate; do not edit by hand

module "intranet" {
  dns_domain_name = "readyset.name"
  source          = "../../../../modules/intranet/global"
}
