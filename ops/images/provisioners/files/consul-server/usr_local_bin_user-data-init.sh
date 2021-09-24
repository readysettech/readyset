#!/usr/bin/env bash
set -eux -o pipefail

setup-data-volume /opt/consul

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
