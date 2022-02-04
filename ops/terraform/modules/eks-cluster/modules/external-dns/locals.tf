locals {
  name = var.dns_zone_mode == "public" ? "externaldns-public" : "externaldns-private"
}
