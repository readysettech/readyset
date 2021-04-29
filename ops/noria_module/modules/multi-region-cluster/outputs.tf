#---------------------------------------------------------------------------------------------------------------------#
# Primary VPC
#---------------------------------------------------------------------------------------------------------------------#

output "primary_zookeeper_public_ip" {
  value       = module.readyset_primary.zookeeper_public_ip
  description = "Zookeper instance public IP. (Primary)"
}

output "primary_readyset_server_public_ip" {
  value       = module.readyset_primary.noria_server_public_ip
  description = "Readyset server instance public IP. (Primary)"
}

output "primary_readyset_mysql_public_ip" {
  value       = module.readyset_primary.noria_mysql_public_ip
  description = "Readyset MySQL instance public IP. (Primary)"
}

output "primary_vpc_id" {
  value       = module.readyset_primary.vpc_id
  description = "The ID of the VPC created in the provided AWS Region. (Primary)"
}

#---------------------------------------------------------------------------------------------------------------------#
# Secondary VPC
#---------------------------------------------------------------------------------------------------------------------#

output "secondary_zookeeper_public_ip" {
  value       = module.readyset_secondary.zookeeper_public_ip
  description = "Zookeper instance public IP. (Secondary)"
}

output "secondary_readyset_server_public_ip" {
  value       = module.readyset_secondary.noria_server_public_ip
  description = "Readyset server instance public IP. (Secondary)"
}

output "secondary_readyset_mysql_public_ip" {
  value       = module.readyset_secondary.noria_mysql_public_ip
  description = "Readyset MySQL instance public IP. (Secondary)"
}

output "secondary_vpc_id" {
  value       = module.readyset_secondary.vpc_id
  description = "The ID of the VPC created in the provided AWS Region. (Secondary)"
}
