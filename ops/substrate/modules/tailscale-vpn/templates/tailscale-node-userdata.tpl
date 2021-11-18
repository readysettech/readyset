#!/bin/bash
set -e

# Setup environment file for systemd service to consume configurations from
cat > /etc/default/tailscale <<EOF
TAILSCALE_ROUTES_ADVERTISED=${advertised_routes}
AUTH_KEY_SECRETS_MANAGER_ARN=${auth_key_secret_arn}
EOF

chmod 600 /etc/default/tailscale

# Restart service to activate configurations
sudo systemctl restart tailscale-autoconnect.service
