#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

cat > /etc/default/telemetry-ingress <<EOF
S3_BUCKET="${S3_BUCKET}"
AUTHORITY="${AUTHORITY}"
EOF
chmod 600 /etc/default/telemetry-ingress

systemctl reset-failed
systemctl enable telemetry-ingress
systemctl start telemetry-ingress
