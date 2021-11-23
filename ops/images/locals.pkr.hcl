locals {
  skip_create_ami        = !var.create_ami
  force_degregister      = !var.production
  force_delete_snapshots = !var.production

  timestamp = timestamp()
  # In production, use a short commit id for versioning, for testing, use a constant
  ami_version = var.production ? substr(var.buildkite_commit, 0, 7) : "dev"
  # Use a timestamp down to the second for a suffix
  ami_suffix = formatdate("YYYYMMDDhhmmss", local.timestamp)

  ami_virtualization_type = "hvm"
  ssh_username            = "ubuntu"

  source_region     = "us-east-2"
  ubuntu_account_id = "099720109477"

  deploy_account_id  = "888984949675"
  build_account_id   = "305232526136"
  sandbox_account_id = "069491470376"
  network_account_id = "911245771907"
  # This defines what accounts besides the one it is created in which are allowed to launch the
  # created AMI images.
  # Deploy is needed so that it can be used to copy images into the deploy account to share.
  # Build is needed so that images developers built in sandbox can be tested in build.
  # Sandbox is needed so that images built in build can be tested in sandbox.
  ami_users           = [local.deploy_account_id, local.build_account_id, local.sandbox_account_id, local.network_account_id]
  destination_regions = ["ap-northeast-1", "eu-west-1", "sa-east-1", "us-east-2", "us-west-2"]
  binaries_path       = var.production ? "${path.root}/binaries/target/release" : "${path.root}/../../target/debug"
}
