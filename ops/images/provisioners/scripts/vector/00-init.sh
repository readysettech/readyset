#!/bin/sh
set -eux

curl -1sLf "https://repositories.timber.io/public/vector/gpg.3543DB2D0A2BC4B8.key" | sudo gpg --dearmor -o /usr/share/keyrings/vector-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/vector-keyring.gpg] https://repositories.timber.io/public/vector/deb/ubuntu $(lsb_release -cs) main" | sudo tee -a /etc/apt/sources.list.d/vector.list

sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y vector=0.20.0

