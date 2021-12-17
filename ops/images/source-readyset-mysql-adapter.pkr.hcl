locals {
  readyset_mysql_adapter_ami_name = format("readyset/images/%s-ssd/readyset-mysql-adapter-%s-amd64-%s",
    local.ami_virtualization_type,
    local.ami_version,
    local.ami_suffix
  )
}

source "amazon-ebs" "readyset-mysql-adapter" {
  # Settings to allow development of images outside of CI.
  skip_create_ami       = local.skip_create_ami
  force_deregister      = local.force_degregister
  force_delete_snapshot = local.force_delete_snapshots

  source_ami_filter {
    owners      = [local.ubuntu_account_id]
    most_recent = true

    filters = {
      name                = format("ubuntu/images/hvm-ssd/ubuntu-%s-*-amd64-server-*", "focal")
      root-device-type    = "ebs"
      virtualization-type = local.ami_virtualization_type
    }
  }
  ami_virtualization_type = local.ami_virtualization_type
  ssh_username            = local.ssh_username


  ami_name                  = local.readyset_mysql_adapter_ami_name
  ssh_clear_authorized_keys = true

  ami_users   = local.ami_users
  region      = local.source_region
  ami_regions = local.ami_regions

  # This is only used for building and has no bearing on how it is deployed
  instance_type = "t2.small"
}
