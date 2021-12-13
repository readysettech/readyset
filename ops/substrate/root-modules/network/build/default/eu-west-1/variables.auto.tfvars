# General
aws_region  = "eu-west-1"
environment = "build"
quality     = "default"

# Tailscale
tailscale_instance_type               = "t3.micro"
tailscale_ssh_allowed_ingress_cidrs   = ["47.198.21.32/32", "10.0.0.0/8"]
tailscale_ssh_access_enabled          = true
tailscale_auth_key_secretsmanager_arn = "arn:aws:secretsmanager:eu-west-1:911245771907:secret:tailscale/AUTHKEY-CRiPGU-ofofyA"
