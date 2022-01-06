locals {
  stack_name    = "buildkite-${var.buildkite_queue}"
  stack_version = "master"
  ssh_key_pair_config = length(var.ssh_key_pair_name) > 0 ? {
    "KeyName" = var.ssh_key_pair_name
  } : {}
  agent_addtl_sudo_perm_config = length(var.agent_additional_sudo_permissions) > 0 ? {
    "BuildkiteAdditionalSudoPermissions" = join(",", var.agent_additional_sudo_permissions)
  } : {}
  subnet_ids = join(",", var.use_private_subnets ? data.aws_subnets.private_subnets.ids : data.aws_subnets.public_subnets.ids)
}
