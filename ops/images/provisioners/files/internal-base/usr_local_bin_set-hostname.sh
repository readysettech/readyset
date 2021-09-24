#!/bin/bash
set -eux

token=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
private_ip=$(curl -H "X-aws-ec2-metadata-token: $token" -v http://169.254.169.254/latest/meta-data/local-ipv4)
hostname_suffix=$(tr "." "-" <<<"${private_ip}")
hostname_prefix=$(cat /usr/local/etc/hostname-prefix)
hostname="${hostname_prefix}-${hostname_suffix}"

hostnamectl --static hostname "${hostname}"
sudo sed -i "s/^\(127\.0\.0\.1\s*localhost\).*/\1 ${hostname}/" /etc/hosts
