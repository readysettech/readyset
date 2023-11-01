## Development

This section is for developers who want to build ReadySet from source as they work on the ReadySet codebase.

### Install prerequisites

1. Install ReadySet dependencies.

   **macOS with [homebrew](https://brew.sh/):**

   ```bash
   brew install lz4 openssl@1.1
   ```

   Add the following to your [cargo config](https://doc.rust-lang.org/cargo/reference/config.html)
   to make it discover `lz4`:

   ```toml
   [env]
   LIBRARY_PATH = "/opt/homebrew/lib"
   ```

   **Ubuntu:**

   ```bash
   sudo apt update && sudo apt install -y build-essential libssl-dev pkg-config llvm clang liblz4-dev
   sudo apt-get -y install cmake
   ```

   **Arch:**

   ```bash
   sudo pacman -S base-devel clang lz4
   ```

   **CentOS/Amazon Linux:**

   ```bash
   sudo yum -y update
   sudo yum -y groupinstall "Development Tools"
   sudo yum -y install clang lz4-devel openssl-devel
   ```

   **Nix:**

   ```bash
   nix-shell
   ```

1. Install Rust via [rustup.rs](https://rustup.rs/).

   ReadySet is written entirely in Rust.

1. Install Docker via [Get Docker](https://docs.docker.com/get-docker/) and Docker Compose via [Install Docker Compose](https://docs.docker.com/compose/install/).

   ReadySet runs alongside a backing Postgres or MySQL database and, when run in distributed fashion, uses Consul for leader election, failure detection, and internal cluster state management. You'll use Docker Compose to create and manage these resources for local development.

### Build and run ReadySet

1. Clone the repo using `git` and navigate into it:

   ```bash
   git clone https://github.com/readysettech/readyset.git
   cd readyset
   ```

1. Start a backing database:

   ```bash
   docker-compose up -d
   ```

   This starts both Postgres and MySQL in containers, although you will run ReadySet against only one at a time. If you don't want to run both databases, edit `docker-compose.yml` and comment out the `mysql` or `postgres` fields.

1. Compile and run ReadySet, replacing `<deployment name>` with a unique identifier for the deployment.

   **Run against Postgres:**

   ```bash
   cargo run --bin readyset --release -- --database-type=postgresql --upstream-db-url=postgresql://postgres:readyset@127.0.0.1:5432/testdb --username=postgres --password=readyset --address=0.0.0.0:5433 --deployment=<deployment name> --prometheus-metrics --query-log-mode all-queries
   ```

   **Run against MySQL:**

   ```bash
   cargo run --bin readyset --release -- --database-type=mysql --upstream-db-url=mysql://root:readyset@127.0.0.1:3306/testdb --username=root --password=readyset --address=0.0.0.0:3307 --deployment=<deployment name> --prometheus-metrics --query-log-mode all-queries
   ```

   This runs the ReadySet Server and Adapter as a single process, a simple, standard way to run ReadySet that is also the easiest approach when developing. For production deployments, however, it is possible to run the Server and Adapter as separate processes. See the [scale out deployment pattern](https://docs.readyset.io/guides/deploy/production-notes/#scale-out) in the docs.

   For details about ReadySet options, see the CLI docs (coming soon).

1. With ReadySet up and running, you can now connect the Postgres or MySQL shell:

   **Postgres:**

   ```bash
   PGPASSWORD=readyset psql \
   --host=127.0.0.1 \
   --port=5433 \
   --username=postgres \
   --dbname=testdb
   ```

   **MySQL:**

   ```bash
   mysql \
   --host=127.0.0.1 \
   --port=3307 \
   --user=root \
   --password=readyset \
   --database=testdb
   ```

### Testing

To run tests for the project, run the following command:

```bash
cargo test --skip integration_serial
```

**Note:**

- Certain tests cannot be run in parallel with others, and these tests are typically in files postfixed with \_serial. Running the entire set of tests for a package (e.g., `cargo test -p readyset-server`) may fail if serial tests are included.

- Running tests may require increasing file descriptor limits. You can do so by running `ulimit -Sn 65535`.


