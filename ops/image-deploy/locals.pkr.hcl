locals {
  skip_create_ami        = !var.create_ami
  force_degregister      = !var.production
  force_delete_snapshots = !var.production

  timestamp = timestamp()

  destination_ami_version = var.production ? substr(var.buildkite_commit, 0, 7) : "release"
  destination_ami_suffix  = formatdate("YYYYMMDDhhmmss", local.timestamp)

  ami_virtualization_type = "hvm"
  ssh_username            = "ubuntu"

  build_account_id   = "305232526136"
  sandbox_account_id = "069491470376"

  source_account = var.production ? local.build_account_id : local.sandbox_account_id

  internal_account_ids = [local.build_account_id, local.sandbox_account_id]
  customer_account_ids = [
    "724964194832", # IRL
    "135195219264", # Kevin Kwok Demo
    "121756176268", # Alex Graham Demo
    "615242630409", # Richard Crowley Demo
    "286292902993", # Joshua Skrypek Demo
    "501778030520", # 2-23 orchestrator walkthrough
  ]

  ami_users = concat(local.internal_account_ids, local.customer_account_ids)

  destination_regions = ["us-east-1", "us-east-2", "us-west-2"]
}
