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

  ubuntu_account_id = "099720109477"

  deploy_account_id = "888984949675"
  build_account_id  = "305232526136"
  # This defines what accounts besides the one it is created in which are allowed to launch the
  # created AMI images. This will allow in the future the deploy account to launch these images
  # there so they can be stored in the deploy account for other users.
  # TODO: Remove build account once initial testing is done as it is being used to test deployment
  ami_users = [local.deploy_account_id, local.build_account_id]

  binaries_path = var.production ? "${path.root}/binaries/target/release" : "${path.root}/../../target/debug"
}
