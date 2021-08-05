output "zookeeper_private_ips" {
  description = "Zookeper instances private IPs"
  value       = module.readyset_tmp.zookeeper_private_ips
}

output "server_private_ips" {
  description = "ReadySet Servers private IPs"
  value       = module.readyset_tmp.zookeeper_private_ips
}

output "db_password" {
  description = "Database Password"
  value       = random_password.db_password.result
  sensitive   = true
}
