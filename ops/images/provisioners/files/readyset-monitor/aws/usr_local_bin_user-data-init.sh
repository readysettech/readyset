#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

on_error() {
  local exit_code="$?"
  journalctl -xe
  /usr/local/bin/cfn-signal-wrapper.sh "$exit_code"
}

trap 'on_error' ERR

/usr/local/bin/cfn-init-wrapper.sh
/usr/local/bin/set-host-description.sh
/usr/local/bin/configure-consul-client.sh
# Possible Race Condition? 🐎
/usr/local/bin/configure-prometheus.sh
/usr/local/bin/configure-grafana.sh

cat > /etc/default/metrics-aggregator <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS:-127.0.0.1:8500}
PROMETHEUS_ADDRESS=127.0.0.1:9091
EOF
chmod 600 /etc/default/metrics-aggregator

cat >> /etc/default/vector <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_TYPE="readyset-monitor"
AWS_CLOUDFORMATION_STACK=${AWS_CLOUDFORMATION_STACK}
AWS_CLOUDFORMATION_REGION=${AWS_CLOUDFORMATION_REGION}
EOF

systemctl reset-failed
systemctl enable vector
systemctl restart vector

systemctl enable metrics-aggregator
systemctl restart metrics-aggregator


/usr/local/bin/cfn-signal-wrapper.sh 0
