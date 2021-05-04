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
2. To get started quickly, clone the test database
  * `git clone git@github.com:readysettech/test_db.git`
3. Run the MySQL adaptor
  * `cd <repo_path>/noria-mysql`
  * `cargo run --release -- --deployment myapp --no-require-authentication --permissive`
3. Load the test database
   * `cd <path to testdb>`
   * `mysql -h 127.0.0.1 < employees.tiny.noria.sql`
4. Make queries! (Note that many SQL queries are NOT supported yet by Noria, and may not fail gracefully either, we're working on improving this!)
  * `mysql -h 127.0.0.1`
  * From inside the shell: `SELECT * FROM employees WHERE emp_no=10001;` (although, note, this is just a sanity check–-- typically you use ReadySet with prepared statements and *then* issue a query with the parameters filled in)


### Start up the ReadySet UI
1. Install Python 3 using these [instructions] (https://www.python.org/downloads/)
2. Install dependencies:
   * On Mac: `sudo pip3 install pystache`
   * On Ubuntu: `apt-get install python-pystache`
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