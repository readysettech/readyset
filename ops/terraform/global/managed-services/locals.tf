locals {
  private_r53_zone_id = (
    var.private_hosted_zone_enabled ?
    aws_route53_zone.private-readyset[0].id :
    var.private_hosted_zone_id
  )
  main_dns_zone_associated_vpcs = {
    "build-default" = {
      vpc_id  = data.aws_vpc.build-default-network.id,
      zone_id = local.private_r53_zone_id,
    }
  }
}
