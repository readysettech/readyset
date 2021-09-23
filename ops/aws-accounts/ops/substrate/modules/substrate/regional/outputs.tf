# managed by Substrate; do not edit by hand

output "tags" {
  value = module.global.tags
}

output "private_subnet_ids" {
  value = module.global.tags.environment == "admin" ? [] : data.aws_subnet_ids.private[0].ids
}

output "public_subnet_ids" {
  value = data.aws_subnet_ids.public.ids
}

output "vpc_id" {
  value = data.aws_vpc.network.id
}
