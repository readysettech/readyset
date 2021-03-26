output "zookeeper_private_ip" {
  value       = aws_instance.zookeeper.private_ip
  description = "Zookeper instance private IP."
}

output "zookeeper_public_ip" {
  value       = aws_instance.zookeeper.public_ip
  description = "Zookeper instance public IP."
}

output "noria_server_public_ip" {
  value       = aws_instance.noria_server.public_ip
  description = "Noria server instance public IP."
}

output "noria_server_private_ip" {
  value       = aws_instance.noria_server.private_ip
  description = "Noria server instance private IP."
}

output "noria_mysql_public_ip" {
  value       = aws_instance.noria_mysql.public_ip
  description = "Noria MySQL instance public IP."
}

output "noria_mysql_private_ip" {
  value       = aws_instance.noria_mysql.private_ip
  description = "Noria MySQL instance private IP."
}

output "kafka_public_ip" {
  value       = aws_instance.kafka.*.public_ip
  description = "Kafka instance public IP."
}

output "kafka_private_ip" {
  value       = aws_instance.kafka.*.private_ip
  description = "Kafka instance private IP."
}

output "debezium_public_ip" {
  value       = aws_instance.debezium.*.public_ip
  description = "Debezium instance public IP."
}

output "debezium_private_ip" {
  value       = aws_instance.debezium.*.private_ip
  description = "Debezium instance private IP."
}

output "debezium_connector_public_ip" {
  value       = aws_instance.debezium_connector.*.public_ip
  description = "Debezium connector instance public IP."
}

output "debezium_connector_private_ip" {
  value       = aws_instance.debezium_connector.*.private_ip
  description = "Debezium connector instance private IP."
}

output "debezium_connect_security_group_id" {
  value       = aws_security_group.debezium.*.id
  description = "ID of the security group created for the Debezium Connect instance. This security group must have access to port 3306 of the RDS DB instance, if specified."
}
