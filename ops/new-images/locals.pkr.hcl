locals {
  skip_create_ami        = !var.create_ami
  force_degregister      = !var.production
  force_delete_snapshots = !var.production

  timestamp = timestamp()
  # In production, use a short commit id for versioning, for testing, use a constant
  ami_version = var.production ? substr(var.buildkite_commit, 0, 7) : "dev"
  # Generate a unique suffix either from the buildkite commit or the exact second this was launched
  unique_ami_suffix = var.buildkite_commit != "" ? var.buildkite_commit : formatdate("YYYYMMDDhhmmss", local.timestamp)
  # In production, use the date as the final element, otherwise, use suffix
  ami_suffix = var.production ? formatdate("YYYYMMDD", local.timestamp) : local.unique_ami_suffix

  ami_virtualization_type = "hvm"
  ssh_username            = "ubuntu"

  source_region = "us-east-2"
}
