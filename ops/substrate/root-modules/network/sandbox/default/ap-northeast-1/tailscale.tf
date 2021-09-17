module "tailscale_ansible" {
  source = "git@github.com:readysettech/tf-module.git//aws/ansible?ref=initial"

  custom_data = jsonencode({
    tailscale_routes = aws_vpc.sandbox-default-ap-northeast-1.cidr_block
  })
  env              = "sandbox"
  role             = "tailscale"
  playbook_version = "v0.0.1"
  vpc              = "sandbox-default"
}

module "tailscale" {
  source = "git@github.com:readysettech/tf-module.git//aws/tailscale?ref=initial"

  ami      = module.tailscale_ansible.ami
  env      = "sandbox"
  ssh_key  = "tailscale"
  template = module.tailscale_ansible.template
  vpc      = "sandbox-default"
}

