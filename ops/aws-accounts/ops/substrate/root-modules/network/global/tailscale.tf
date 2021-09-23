module "tailscale" {
  source = "git@github.com:readysettech/tf-module.git//aws/iam/instance?ref=initial"

  env         = "sandbox"
  policy_arns = [module.ansible.policy_arn]
  role        = "tailscale"
}
