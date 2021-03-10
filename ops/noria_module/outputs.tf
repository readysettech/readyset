output "zookeeper_private_ip" {
  value = aws_instance.zookeeper.private_ip
}

output "noria_server_public_ip" {
  value = aws_instance.noria_server.private_ip
}

output "noria_mysql_private_ip" {
  value = aws_instance.noria_mysql.private_ip
}
