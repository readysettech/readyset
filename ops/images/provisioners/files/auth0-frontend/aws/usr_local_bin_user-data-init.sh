#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

cat > /etc/default/auth0-frontend <<EOF
CLIENT_ID="${CLIENT_ID}"
ISSUER_BASE_URL="${ISSUER_BASE_URL}"
SECRET="${SECRET}"
BASE_URL="${BASE_URL}"
PORT=3000
EOF
chmod 600 /etc/default/auth0-frontend

systemctl reset-failed
systemctl enable auth0-frontend
systemctl start auth0-frontend
