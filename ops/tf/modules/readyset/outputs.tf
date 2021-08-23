output "region" {
  description = "The region of the VPC"
  value       = data.aws_region.current.name
}

output "vpc_cidr_block" {
  description = "CIDR block of the VPC"
  value       = data.aws_vpc.vpc.cidr_block
}

output "zookeeper_private_ips" {
  description = "Zookeper instances private IPs"
  value       = aws_network_interface.zookeeper.*.private_ip
}

output "server_private_ips" {
  description = "ReadySet Servers private IPs"
  value       = aws_instance.server.*.private_ip
}

output "deployment" {
  description = "ReadySet deployment name"
  value       = var.deployment
}
