#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Set the hostname before starting tailscale
sudo hostnamectl set-hostname ${machine_hostname}

# Setup environment file for systemd service to consume configurations from
echo "Beginning Tailscale subnet router configuration process."
cat > /etc/default/tailscale <<EOF
TAILSCALE_ROUTES_ADVERTISED=${advertised_routes}
AUTH_KEY_SECRETS_MANAGER_ARN=${auth_key_secret_arn}
EOF

# Set env perms
echo "Configured env file successfully. Modifying permissions."
sudo chmod 600 /etc/default/tailscale

# Restart service to activate configurations
echo "Permissions set. Restarting service."
sudo systemctl restart tailscale-autoconnect.service

echo "Service restart complete!"
