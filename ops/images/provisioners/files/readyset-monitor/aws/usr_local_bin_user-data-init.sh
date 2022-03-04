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
/usr/local/bin/configure-prometheus.sh
/usr/local/bin/configure-grafana.sh

cat > /etc/default/metrics-aggregator <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
AUTHORITY_ADDRESS=${AUTHORITY_ADDRESS:-127.0.0.1:8500}
PROMETHEUS_ADDRESS=127.0.0.1:9091
EOF
chmod 600 /etc/default/metrics-aggregator

cat >> /etc/vector.d/env <<EOF
NORIA_DEPLOYMENT=${DEPLOYMENT}
NORIA_TYPE="readyset-monitor"
EOF

cat > /etc/vector.d/vector.toml <<EOF
[sources.in]
type = "vector"
address = "0.0.0.0:9000"

[sources.node-exporter]
type = "prometheus_scrape"
endpoints = [ "http://localhost:9100/metrics" ]
scrape_interval_secs = 15

[transforms.metrics]
 type = "remap"
 inputs = ["node-exporter"]
 source = '''
   .tags.deployment = "${DEPLOYMENT}"
   .tags.job = "readyset-monitor"
'''

[transforms.app_logs]
  type = "remap"
  inputs = ["in"]
  source = '''
    if !exists(.app) {
      if exists(.SYSLOG_IDENTIFIER) {
        .app = .SYSLOG_IDENTIFIER
      } else {
        .app = "systemlogs"
      }
    }
  '''

[sinks.out]
inputs = ["in"]
type = "console"
target = "stdout"
encoding.codec = "json"

[sinks.cloudwatch_logs]
type = "aws_cloudwatch_logs"
inputs = ["app_logs"]
create_missing_group = true
create_missing_stream = true
group_name = "${AWS_CLOUDFORMATION_STACK}"
compression = "none"
region = "${AWS_CLOUDFORMATION_REGION}"
stream_name = "{{ app }}"
encoding.codec = "json"

[sinks.cloudwatch_metrics]
type = "aws_cloudwatch_metrics"
inputs = ["in", "metrics"]
default_namespace = "${DEPLOYMENT}"
region = "${AWS_CLOUDFORMATION_REGION}"

[sinks.prometheus]
type = "prometheus_exporter"
inputs = ["in", "metrics"]
address = "0.0.0.0:9090"
EOF

# Drop errors from vector which can flakily fail to start
# if vector fails to open a prometheus scrape endpoint.
/usr/local/bin/configure-vector.sh || true

systemctl reset-failed
systemctl enable metrics-aggregator
systemctl restart metrics-aggregator

/usr/local/bin/cfn-signal-wrapper.sh 0
