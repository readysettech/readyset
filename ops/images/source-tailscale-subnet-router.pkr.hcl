locals {
  tailscale_subnet_router_ami_name = format(
    "readyset/images/%s-ssd/tailscale-subnet-router-%s-amd64-%s",
    local.ami_virtualization_type,
    local.ami_version,
    local.ami_suffix
  )
}

source "amazon-ebs" "tailscale-subnet-router" {
  # Settings to allow development of images outside of CI.
  skip_create_ami       = local.skip_create_ami
  force_deregister      = local.force_degregister
  force_delete_snapshot = local.force_delete_snapshots

  # TODO: Make the source AMI internal-base after making two step pipeline
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

  ami_name                  = local.tailscale_subnet_router_ami_name
  ssh_clear_authorized_keys = true

  ami_users = local.ami_users
  region    = local.source_region

  ami_regions = local.destination_regions

  # This is only used for building and has no bearing on how it is deployed
  instance_type = "t3.small"

  metadata_options {
    http_tokens = "required"
  }
}
