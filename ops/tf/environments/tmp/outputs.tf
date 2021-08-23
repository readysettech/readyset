output "zookeeper_private_ips" {
  description = "Zookeper instances private IPs"
  value       = module.readyset_tmp.zookeeper_private_ips
}

output "server_private_ips" {
  description = "ReadySet Servers private IPs"
  value       = module.readyset_tmp.server_private_ips
}

output "db_password" {
  description = "Database Password"
  value       = random_password.db_password.result
  sensitive   = true
}

output "db_endpoint" {
  description = "Database exposed endpoint"
  value       = module.readyset_tmp.db_endpoint
}

output "deployment" {
  description = "ReadySet deployment name"
  value       = module.readyset_tmp.deployment
}
