locals {
  readyset_mysql_adapter_destination_ami_name = format("readyset/images/%s-ssd/readyset-mysql-adapter-%s-amd64-%s",
    local.ami_virtualization_type,
    local.destination_ami_version,
    local.destination_ami_suffix
  )

  readyset_mysql_adapter_region_ami_id = split(":", var.readyset_mysql_adapter_region_ami_id)
  readyset_mysql_adapter_source_ami_id = local.readyset_mysql_adapter_region_ami_id[1]
  readyset_mysql_adapter_source_region = local.readyset_mysql_adapter_region_ami_id[0]
}

source "amazon-ebs" "readyset-mysql-adapter" {
  # Settings to allow development of images outside of CI.
  skip_create_ami       = local.skip_create_ami
  force_deregister      = local.force_degregister
  force_delete_snapshot = local.force_delete_snapshots

  ssh_username            = local.ssh_username
  ami_virtualization_type = local.ami_virtualization_type

  source_ami = local.readyset_mysql_adapter_source_ami_id
  region     = local.readyset_mysql_adapter_source_region

  ami_name = local.readyset_mysql_adapter_destination_ami_name

  # This is only used for starting up and then shutting down again
  instance_type = "t3.micro"

  ami_regions = local.destination_regions
  ami_users   = local.ami_users
}

build {
  sources = ["source.amazon-ebs.readyset-mysql-adapter"]
  post-processor "manifest" {}
}
