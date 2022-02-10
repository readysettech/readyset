#-------------- [ Route53 ] ------------------------------------------- #
# Primary domain for internal DNS within AWS
# Always deployed into the "admin" account
resource "aws_route53_zone" "private-readyset" {
  count    = var.private_hosted_zone_enabled ? 1 : 0
  provider = aws.dns
  name     = var.private_hosted_zone_name

  # Due to Subsrate's shared VPC model, we needed to create
  # a VPC that actually lives in the admin account.
  # This is a limitation in AWS for shared VPCs and private hosted zone association/authorization.
  vpc {
    vpc_id     = data.aws_vpc.admin-blackhole-network.id
    vpc_region = var.aws_region
  }

  # Prevent the deletion of associated VPCs after
  # the initial creation. See documentation on
  # aws_route53_zone_association for details
  lifecycle {
    ignore_changes = [vpc]
  }
}

resource "aws_route53_vpc_association_authorization" "private-readyset" {
  for_each = local.main_dns_zone_associated_vpcs
  # Authorizations must come from AWS account hosting the private zone
  provider = aws.dns
  vpc_id   = each.value.vpc_id
  zone_id  = local.private_r53_zone_id
}

resource "aws_route53_zone_association" "dns-enabled-vpcs" {
  for_each = local.main_dns_zone_associated_vpcs
  # Association must be performed in account that owns VPC
  provider = aws.network
  vpc_id   = aws_route53_vpc_association_authorization.private-readyset[each.key].vpc_id
  zone_id  = aws_route53_vpc_association_authorization.private-readyset[each.key].zone_id
}
