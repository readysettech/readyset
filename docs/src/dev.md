## Troubleshooting

1. `cargo build --release` causes an error: `error:invalid channel name 'nightly-2020-11-15-x86_64-unknown-linux-gnu' in '/home/ubuntu/noria/rust-toolchain'`
- Noria is pinned to a version of Rust that you have not installed, here: [noria/rust-toolchain](https://github.com/readysettech/noria/blob/master/rust-toolchain). Install the listed version with:`$rustup toolchain install <insert channel name here>`

## Building binaries with a specific ubuntu version

Building binaries locally, and then trying to use them in a remote virtual
machine may cause issues if your local glibc version is newer than the glibc
version in the virtual machine. To help aleviate this issue, we have scripts
which will build a docker container for a specific version of ubuntu, and then
run cargo inside of this container for you. You use it in place of `cargo`, and
it will create a `target-ubuntu2004` folder at the root of the monorepo where
your built binaries will live.

Currently all supported environments will be found inside the `build` folder.
For example, if you would like to build for the ubuntu-20.04 release, you can
use the script found in the `build/ubuntu20.04/cargo-ubuntu2004` file, and
simply pass it the arguments you would normally pass cargo.

```sh
$ ./build/ubuntu20.04/cargo-ubuntu2004 build --bin=noria-server
```

You will then find the release binaries inside of the `target-ubuntu2004`
directory at the root of the monorepo.

You can also symlink any of these scripts to make them easier to execute.
Example:

```sh
$ ln -s $(pwd)/build/ubuntu20.04/cargo-ubuntu2004 $HOME/bin/cargo-ubuntu2004
$ cargo-ubuntu2004 build --bin=noria-server
```

These binaries can also be used directly as part of a test packer build by
setting the `BINARIES_PATH` environment variable, for example:

```sh
$ cd ops/images
$ BINARIES_PATH=../../target-ubuntu2004/debug PACKER_CREATE_AMI=true packer build -only=readyset-* .
```

### Arch Setup Script (Out-of-date).

If you're using Arch linux, you can use the following setup script:

```bash
#!/bin/bash

set -e

echo Welcome to ReadySet! Let\s get you setup!
read -p 'Clone readyset git repo to: ' gitrepo

#####################################
##    General Work Dependencies    ##
#####################################

## Start with repo update & pkg upgrade
sudo pacman -Syu

# Install tailscale for work vpn
sudo pacman -S --needed --noconfirm tailscale

# enable and start tailscale
sudo systemctl enable --now tailscaled.service
sleep 1
sudo systemctl start tailscaled.service
sleep 1

# Bring tailscale up - note, this will require you clicking a link and
# authenticating with SSO login
sudo tailscale up

#####################################
## ReadySet (Product) Dependencies ##
#####################################

# Install rust
curl https://sh.rustup.rs -sSf | sh
source $HOME/.cargo/env

# Install nightly rust
rustup install nightly

# Install RLS
rustup component add rls rust-analysis rust-src

# Install mdbook
cargo install mdbook

# Install all basic deps
## OpenSSL is already installed by default on Arch systems.
sudo pacman -S --needed --noconfirm base-devel clang lz4 docker

# Enable and start docker service
sudo systemctl enable docker.service
sudo systemctl start docker.service

# Add current user to the docker group
sudo gpasswd -a $(whoami) docker

# Install MariaDB clients
sudo pacman -S mariadb-clients

# Clone readyset repo
eval cd "${gitrepo}"
git clone git@github.com:readysettech/readyset.git
cd readyset

# Build release
cargo build --release

# Install UI deps
sudo pacman -S --noconfirm --needed python-pip
pip3 install pystache

echo 'All finished! Please add $HOME/.cargo/bin to your PATH permanentely.'
```
