module "tailscale" {
  source = "../../../../../modules/tailscale-vpn"
  count  = var.tailscale_enabled ? 1 : 0
  # General
  environment   = var.environment
  resource_tags = var.resource_tags
  quality       = var.quality

  # Instance Configs
  aws_region    = var.aws_region
  ami_id        = var.tailscale_ami_id
  instance_type = var.tailscale_instance_type

  root_volume_configs = {
    volume_size           = var.tailscale_root_volume_size
    delete_on_termination = var.tailscale_root_volume_del_on_term
  }

  # Networking
  vpc_id                    = aws_vpc.sandbox-default-eu-west-1.id
  ts_cfg_advertised_routes  = [aws_vpc.sandbox-default-eu-west-1.cidr_block]
  ssh_access_enabled        = var.tailscale_ssh_access_enabled
  ssh_allowed_ingress_cidrs = var.tailscale_ssh_allowed_ingress_cidrs

  # Security
  key_pair_name                              = var.tailscale_keypair_name
  iam_authorized_secrets_manager_arn         = var.tailscale_auth_key_secretsmanager_arn
  iam_authorized_secrets_manager_kms_key_arn = var.tailscale_secretsmanager_kms_key_arn

}
