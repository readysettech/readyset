#!/usr/bin/env bash
set -eux -o pipefail

sudo DEBIAN_FRONTEND=noninteractive apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nginx

sudo install -o root -g root -m 644 \
    /tmp/auth0-frontend/etc_nginx_nginx.conf \
    /etc/nginx/nginx.conf

# Check that the nginx config is valid, fail if not
sudo /usr/sbin/nginx -c /etc/nginx/nginx.conf -t

sudo systemctl enable nginx
