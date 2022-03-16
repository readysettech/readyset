#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

on_error() {
  local exit_code="$?"

  /usr/local/bin/cfn-signal-wrapper.sh "$exit_code"
}

trap 'on_error' ERR

/usr/local/bin/cfn-init-wrapper.sh
/usr/local/bin/set-host-description.sh
/usr/local/bin/configure-consul-client.sh
# Build connection string from inputs
source /usr/local/bin/get-connection-string.sh
UPSTREAM_DB_URL=${DB_URL}

cat > /etc/default/readyset-psql-adapter <<EOF
UPSTREAM_DB_URL=${UPSTREAM_DB_URL}
NORIA_DEPLOYMENT=${DEPLOYMENT}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS:-127.0.0.1:8500}
ALLOWED_USERNAME=${USERNAME}
ALLOWED_PASSWORD=${PASSWORD}
LOG_LEVEL=${LOG_LEVEL:-info}
EOF
chmod 600 /etc/default/readyset-psql-adapter

IMDS_TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
SERVER_ADDRESS=$(curl -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

cat >> /etc/vector.d/env <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_TYPE="readyset-adapter"
SERVER_ADDRESS=${SERVER_ADDRESS}
EOF

/usr/local/bin/configure-vector.sh || true

systemctl reset-failed
systemctl enable readyset-psql-adapter
systemctl restart readyset-psql-adapter

/usr/local/bin/cfn-signal-wrapper.sh 0
