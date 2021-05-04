# Setting up your development environment

### Install dependencies
1. Install RustUp using the instructions [here](https://www.rust-lang.org/tools/install)
2. Install Docker using the instructions [here](https://docs.docker.com/get-docker/)
3. Install Readyset dependencies:
  * On Mac:
    - `brew install lz4`
    - `brew install openssl@1.1`
  * On Ubuntu:
    - `sudo apt update && sudo apt install -y build-essential libssl-dev pkg-config llvm clang liblz4-dev`
    - `sudo apt-get -y install cmake`
  * On Arch:
    - `sudo pacman -S base-devel clang lz4`
4. Optionally for Arch users, you can use the setup script listed at the bottom
   of this document.

### Clone, build, and run ReadySet
1. Clone the repo:
   * `git clone git@github.com:readysettech/readyset.git`
   * Note: If this fails because you have not configured an SSH key for Github, instructions are [here](https://docs.github.com/en/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account)
2. Build ReadySet
   * `cd readyset`
   * `cargo build --release` (leave off the release flag if you plan on developing, it optimizes the code but increases build times and doesn't give you a meaningful stack trace)
3. Start Zookeeper
  * `sudo docker run --name zookeeper --restart always -p 2181:2181 -d zookeeper`
4. Start ReadySet
  * `cargo r --release --bin noria-server -- --deployment myapp --no-reuse --address 127.0.0.1 --shards 0`

### Interact with ReadySet via the MySQL adapter
1. Install a MySQL shell of your choosing.
   * on Mac: `brew install mysql`
   * on Arch: `sudo pacman -S mariadb-clients`
2. To get started quickly, clone the test database
  * `git clone git@github.com:readysettech/test_db.git`
3. Run the MySQL adaptor
  * `cd <repo_path>/noria-mysql`
  * `cargo run --release -- --deployment myapp --no-require-authentication --permissive`
4. Load the test database
   * `cd <path to testdb>`
   * `mysql -h 127.0.0.1 < employees.tiny.noria.sql`
5. Make queries! (Note that many SQL queries are NOT supported yet by Noria, and may not fail gracefully either, we're working on improving this!)
  * `mysql -h 127.0.0.1`
  * From inside the shell: `SELECT * FROM employees WHERE emp_no=10001;` (although, note, this is just a sanity check–-- typically you use ReadySet with prepared statements and *then* issue a query with the parameters filled in)


### Start up the ReadySet UI
1. Install Python 3 using these [instructions] (https://www.python.org/downloads/)
2. Install dependencies:
   * On Mac: `pip3 install pystache`
   * On Ubuntu: `apt-get install python-pystache`
   * On Arch: `pip3 install pystache`
3. Make, and run the UI:
   * `cd <repo_path>/noria-ui` (from the repository root)
   * `make`
   * `/run.sh`
4. Get the external IP of Noria
  * `cargo run --bin noria-zk --  --show --deployment myapp | grep external | cut -d' ' -f4`
5. Connect the Noria UI to the relevant controller
  * Navigate to [localhost:8000]
  * Press "Click to connect to controller" in the top left of the screen


### Clean up 
- Noria server can be stopped via `CTRL-C`
- Use `docker container rm <container-name>` to remove stopped containers. Add `-f` if you want to stop and remove container that is still running. You can remove the zookeeper container to fully clean out the noria server + database you are working with.

### Troubleshooting

1. `cargo build --release` causes an error: `error:invalid channel name 'nightly-2020-11-15-x86_64-unknown-linux-gnu' in '/home/ubuntu/noria/rust-toolchain'`
- Noria is pinned to a version of Rust that you have not installed, here: [noria/rust-toolchain](https://github.com/readysettech/noria/blob/master/rust-toolchain). Install the listed version with:`$rustup toolchain install <insert channel name here>`

### Arch Setup Script

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
