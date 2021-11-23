# General
aws_region  = "eu-west-1"
environment = "sandbox"
quality     = "default"

# Tailscale
tailscale_ami_id                      = "ami-045b74d8d818b5a3e"
tailscale_instance_type               = "t3.micro"
tailscale_ssh_allowed_ingress_cidrs   = ["47.201.146.229/32", "10.0.0.0/8"]
tailscale_ssh_access_enabled          = true
tailscale_auth_key_secretsmanager_arn = "arn:aws:secretsmanager:eu-west-1:911245771907:secret:tailscale/AUTHKEY-CRiPGU-ofofyA"
# ARN for the default aws/ssm key in the region
tailscale_secretsmanager_kms_key_arn = "arn:aws:kms:us-east-2:069491470376:key/5cb3afeb-e9dd-40df-98cf-a82bb53ee78b"
