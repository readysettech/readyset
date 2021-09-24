locals {
  skip_create_ami        = !var.create_ami
  force_degregister      = !var.production
  force_delete_snapshots = !var.production

  timestamp = timestamp()

  destination_ami_version = var.release_name != "" ? var.release_name : "release-test"
  destination_ami_suffix  = var.production ? formatdate("YYYYMMDD", local.timestamp) : formatdate("YYYYMMDDhhmmss", local.timestamp)

  ami_virtualization_type = "hvm"
  ssh_username            = "ubuntu"

  build_account_id   = "305232526136"
  sandbox_account_id = "069491470376"

  source_account = var.production ? local.build_account_id : local.sandbox_account_id
  source_region  = "us-east-2"

  internal_account_ids = [local.build_account_id, local.sandbox_account_id]
  customer_account_ids = []

  ami_users = concat(local.internal_account_ids, local.customer_account_ids)

  destination_regions = ["us-west-2"]
}
