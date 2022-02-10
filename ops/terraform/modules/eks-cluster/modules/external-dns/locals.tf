locals {
  name             = var.dns_zone_mode == "public" ? local.pub_dns_name : local.private_dns_name
  pub_dns_name     = format("extdns-pub-%s", replace(var.r53_domain, ".", "-"))
  private_dns_name = format("extdns-priv-%s", replace(var.r53_domain, ".", "-"))
  dns_role_assume  = var.external_dns_cross_account_zone ? module.ext-dns-cross-account-zone-role.iam_role_arn : ""
}
