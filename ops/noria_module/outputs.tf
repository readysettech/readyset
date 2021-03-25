output "zookeeper_private_ip" {
  value       = aws_instance.zookeeper.private_ip
  description = "Zookeper instance private IP."
}

output "noria_server_public_ip" {
  value       = aws_instance.noria_server.public_ip
  description = "Noria server instance public IP."
}

output "noria_mysql_private_ip" {
  value       = aws_instance.noria_mysql.private_ip
  description = "Noria MySQL instance private IP."
}

output "debezium_connect_security_group_id" {
  value       = aws_security_group.debezium.*.id
  description = "ID of the security group created for the Debezium Connect instance. This security group must have access to port 3306 of the RDS DB instance, if specified."
}
