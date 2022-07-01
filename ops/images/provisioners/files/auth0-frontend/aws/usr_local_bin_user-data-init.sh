#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

get_secret_string_by_id () {
  echo "$(aws secretsmanager get-secret-value --secret-id $1 --cli-connect-timeout $2 | jq -r .SecretString)"
}

# Don't expose secret value in logs
set +e
AUTH0_CLIENT_SECRET="$(get_secret_string_by_id "${AUTH0_READYSET_APP_CLIENT_SECRET_ARN}" 1)"
ec=$?
set -e

# Validation
if [ $ec -ne 0 ]; then
    echo "Failed to retrieve auth key from secret: ${AUTH0_READYSET_APP_CLIENT_SECRET_ARN}. Exit code: ${ec}"
    exit 1
elif [ -z "${AUTH0_READYSET_APP_CLIENT_SECRET_ARN}" ]; then
    echo "Unexpected value retrieved from secret: ${AUTH0_READYSET_APP_CLIENT_SECRET_ARN}. Check the value to be sure it's as expected. Exiting with failure."
    exit 1
fi

AUTH0_SECRET=$(openssl rand -hex 32)
SESSION_SECRET=$(openssl rand -hex 32)

cat > /etc/default/auth0-frontend <<EOF
NEXT_PUBLIC_AUTH0_CLIENT_ID="${CLIENT_ID}"
NEXT_PUBLIC_AUTH0_SCOPE="openid profile"
NEXT_PUBLIC_AUTH0_DOMAIN="${DOMAIN}"
NEXT_PUBLIC_BASE_URL="${ISSUER_BASE_URL}"
NEXT_PUBLIC_REDIRECT_URI="${REDIRECT_URI}"
NEXT_PUBLIC_POST_LOGOUT_REDIRECT_URI="${LOGOUT_URI}"
AUTH0_SECRET="${AUTH0_SECRET}"
AUTH0_CLIENT_SECRET="${AUTH0_CLIENT_SECRET}"
SESSION_COOKIE_SECRET="${SESSION_SECRET}"
SESSION_COOKIE_LIFETIME=7200
EOF

chmod 600 /etc/default/auth0-frontend

systemctl reset-failed
systemctl enable auth0-frontend
systemctl start auth0-frontend