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
/usr/local/bin/configure-consul-client.sh
/usr/local/bin/configure-vector.sh

cat > /etc/vector.d/vector.toml <<EOF
[sources.in]
type = "vector"
address = "0.0.0.0:9000"

[sinks.out]
inputs = ["in"]
type = "console"
target = "stdout"
encoding.codec = "text"

[sinks.cloudwatch]
type = "aws_cloudwatch_logs"
inputs = ["in"]
create_missing_group = true
create_missing_stream = true
group_name = "${AWS_CLOUDFORMATION_STACK}"
compression = "none"
region = "${AWS_CLOUDFORMATION_REGION}"
stream_name = "readyset"
encoding.codec = "json"

[sinks.cloudwatch-metrics]
type = "aws_cloudwatch_metrics"
inputs = ["in"]
default_namespace = "${AWS_CLOUDFORMATION_STACK}"
compression = "none"
region = "us-east-2"
EOF

usr/local/bin/cfn-signal-wrapper.sh 0
