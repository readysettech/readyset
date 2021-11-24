#!/bin/bash

# Borrowed from https://tailscale.com/kb/1096/nixos-minecraft/

# Wait for tailscaled to settle
sleep 2

# Check to see if we are already authenticated to Tailscale
status="$(/usr/bin/tailscale status -json | /usr/bin/jq -r .BackendState)"
if [ "$status" = "Running" ]; then # if so, then do nothing
    exit 0
fi

if [ -z "${AUTH_KEY_SECRETS_MANAGER_ARN}" ]; then
    echo "Value not supplied to required environment variable: AUTH_KEY_SECRETS_MANAGER_ARN. Exiting with failure."
    exit 1
elif [ -z "${TAILSCALE_ROUTES_ADVERTISED}" ]; then
    echo "Value not supplied to required environment variable: TAILSCALE_ROUTES_ADVERTISED. Exiting with failure."
    exit 1
fi

# Retrieve Tailscale auth key from SSM/Secrets Manager
AUTH_KEY_VALUE=$(aws ssm get-parameter --name ${AUTH_KEY_SECRETS_MANAGER_ARN} --with-decryption --output text --query Parameter.Value)

ec=$?
if [ $ec -ne 0 ]; then
    echo "Failed to retrieve auth key from secret: ${AUTH_KEY_SECRETS_MANAGER_ARN}. Exit code: ${ec}"
    exit 1
elif [ -z "${AUTH_KEY_VALUE}" ]; then
    echo "Unexpected value retrieved from secret: ${AUTH_KEY_SECRETS_MANAGER_ARN}. Check the value to be sure it's as expected. Exiting with failure."
    exit 1
fi

echo "Starting up Tailscale service."
/usr/bin/tailscale up \
--advertise-routes ${TAILSCALE_ROUTES_ADVERTISED} \
--authkey ${AUTH_KEY_VALUE}
