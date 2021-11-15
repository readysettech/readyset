#!/usr/bin/env bash
set -eux -o pipefail

# https://aws.amazon.com/premiumsupport/knowledge-center/ec2-linux-log-user-data/
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

on_error() {
  local exit_code="$?"

  /usr/local/bin/cfn-signal-wrapper.sh "$exit_code"
}

trap 'on_error' ERR

/usr/local/bin/set-host-description.sh
/usr/local/bin/cfn-init-wrapper.sh

mkdir -p /opt/consul
# TODO: Maintain state in a separate EBS volume
# setup-data-volume /opt/consul

cat > /etc/consul.d/consul.hcl <<EOF
data_dir = "/opt/consul"
client_addr = "0.0.0.0"
server = true
bootstrap_expect=${CONSUL_BOOTSTRAP_EXPECT:-1}
retry_join = ["provider=aws tag_key=${CONSUL_TAG_KEY:-consul-server} tag_value=${CONSUL_TAG_VALUE}"]
EOF

systemctl reset-failed
systemctl enable consul.service
systemctl restart consul.service

/usr/local/bin/cfn-signal-wrapper.sh 0
