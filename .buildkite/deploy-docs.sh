#!/usr/bin/env bash

# we're using bash
# shellcheck disable=SC3044

docs_instance_private_ip="10.3.152.142"
docs_instance_host_key="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIILIn1W7g3Iq68M7y00KVklY+0Q5klDB8yUIby5qQ3r2"

echo '--- :rust: Building rustdoc'
cargo --locked doc --workspace --document-private-items

echo '--- :rust: Running mdbook'
curl -sSL https://github.com/rust-lang/mdBook/releases/download/v0.4.15/mdbook-v0.4.15-x86_64-unknown-linux-gnu.tar.gz \
    | tar -xvzf - -C /tmp
pushd docs || exit
/tmp/mdbook build

echo '--- :rust: Deploying docs'
mkdir -p book/rustdoc
mv ../target/doc/* book/rustdoc/
mkdir -p ~/.ssh
echo "$docs_instance_private_ip $docs_instance_host_key" > ~/.ssh/known_hosts
rsync --delete -rzPve ssh book/ "admin@${docs_instance_private_ip}:/var/www/html/"
