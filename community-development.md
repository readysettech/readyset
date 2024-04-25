## Development

This section is for developers who want to build Readyset from source as they work on the Readyset
codebase.  It is recommended to build Readyset on a system with at least 4 cores, 8GB of RAM, and 20GB of
storage.

### Install prerequisites

1. Install Readyset dependencies.

   **macOS with [homebrew](https://brew.sh/):**

   ```bash
   brew install lz4 openssl
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

   Readyset is written entirely in Rust.

1. Install Docker via [Get Docker](https://docs.docker.com/get-docker/) and Docker Compose via [Install Docker Compose](https://docs.docker.com/compose/install/).

   Readyset runs alongside a backing Postgres or MySQL database and, when run in distributed fashion, uses Consul for leader election, failure detection, and internal cluster state management. You'll use Docker Compose to create and manage these resources for local development.

### Build and run Readyset

1. Clone the repo using `git` and navigate into it:

   ```bash
   git clone https://github.com/readysettech/readyset.git
   cd readyset
   ```

1. Start a backing database:

   ```bash
   docker-compose up -d
   ```

   This starts both Postgres and MySQL in containers, although you will run Readyset against only one at a time. If you don't want to run both databases, edit `docker-compose.yml` and comment out the `mysql` or `postgres` fields.

1. Compile and run Readyset, replacing `<deployment name>` with a unique identifier for the deployment.

   **Run against Postgres:**

   ```bash
   cargo run --bin readyset --release -- --database-type=postgresql --upstream-db-url=postgresql://postgres:readyset@127.0.0.1:5432/testdb --username=postgres --password=readyset --address=0.0.0.0:5433 --deployment=<deployment name> --prometheus-metrics
   ```

   **Run against MySQL:**

   ```bash
   cargo run --bin readyset --release -- --database-type=mysql --upstream-db-url=mysql://root:readyset@127.0.0.1:3306/testdb --username=root --password=readyset --address=0.0.0.0:3307 --deployment=<deployment name> --prometheus-metrics
   ```

   This runs the Readyset Server and Adapter as a single process, a simple, standard way to run Readyset that is also the easiest approach when developing. For production deployments, however, it is possible to run the Server and Adapter as separate processes. See the [scale out deployment pattern](https://docs.readyset.io/guides/deploy/production-notes/#scale-out) in the docs.

   For details about Readyset options, see the CLI docs (coming soon).

1. With Readyset up and running, you can now connect the Postgres or MySQL shell:

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


### Building binary packages

Linux distribution packages can be built that will install Readyset as a `systemd` service.  The
current configurations have been tested with Fedora 39, Ubuntu 22.04, Debian 12, and Amazon Linux
2023. However, they may work just fine on other distros as well.  In addition to the set-up steps
for building Readyset, the following dependencies are required:

For `deb`:
```bash
    cargo install cargo-deb
```

For `rpm`:
```bash
    cargo install cargo-generate-rpm
```

To build a stripped `readyset` binary:
```bash
    cargo --locked build --profile=release-dist-quick  --bin readyset
```

After building, to generate a `deb` package run:
```bash
    cargo deb --no-build --profile=release-dist-quick -p readyset
```

To generate an `rpm` package run:
```bash
    cargo generate-rpm --profile=release-dist-quick -p readyset \
         --metadata-overwrite=readyset/pkg/rpm/scriptlets.toml
```

To install and use `readyset` from the binary packages, see [the instructions on our main documentation site][binpkgs].

[binpkgs]: https://docs.readyset.io/get-started/install-rs/binaries/install-package
