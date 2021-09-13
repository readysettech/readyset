#!/bin/sh

# Borrowed from https://tailscale.com/kb/1096/nixos-minecraft/

# Wait for tailscaled to settle
sleep 2

# Check to see if we are already authenticated to Tailscale
status="$(/usr/bin/tailscale status -json | /usr/bin/jq -r .BackendState)"
if [ "$status" = "Running" ]; then # if so, then do nothing
    exit 0
fi

# TODO: Make tailscale advertise route value configurable
/usr/bin/tailscale up \
-authkey "$(aws ssm get-parameter --name /aws/reference/secretsmanager/tailscale/AUTHKEY --with-decryption --output text --query Parameter.Value)"
