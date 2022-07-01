#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
get_secret_string_by_id () {
  echo "$(aws secretsmanager get-secret-value --secret-id $1 --cli-connect-timeout $2 | jq -r .SecretString)"
}

set +e
AUTH0_CLIENT_SECRET="$(get_secret_string_by_id "${auth0_rs_app_client_secret_arn}" 1)"
ec=$?
set -e

# Validation
if [ $ec -ne 0 ]; then
    echo "Failed to retrieve auth key from secret: ${auth0_rs_app_client_secret_arn}. Exit code: $ec"
    exit 1
elif [ -z "${auth0_rs_app_client_secret_arn}" ]; then
    echo "Unexpected value retrieved from secret: ${auth0_rs_app_client_secret_arn}. Check the value to be sure it's as expected. Exiting with failure."
    exit 1
fi

cat > /etc/default/auth0-frontend <<EOF
NEXT_PUBLIC_AUTH0_CLIENT_ID="${auth0_client_id}"
NEXT_PUBLIC_AUTH0_SCOPE="openid profile"
NEXT_PUBLIC_AUTH0_AUDIENCE=${auth0_audience}
NEXT_PUBLIC_AUTH0_DOMAIN="${auth0_domain}"
NEXT_PUBLIC_BASE_URL="${issuer_base_url}"
NEXT_PUBLIC_REDIRECT_URI="${redirect_uri}"
NEXT_PUBLIC_POST_LOGOUT_REDIRECT_URI="${logout_uri}"
AUTH0_SECRET="${auth0_secret}"
AUTH0_CLIENT_SECRET="$AUTH0_CLIENT_SECRET"
SESSION_COOKIE_SECRET="${session_secret}"
SESSION_COOKIE_LIFETIME=7200
EOF

chmod 600 /etc/default/auth0-frontend

systemctl reset-failed
systemctl enable auth0-frontend
systemctl start auth0-frontend
