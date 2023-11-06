# Development Guide

## Prerequisites

### Install Dependencies

Prior to running ReadySet, you may need to install the following dependencies:
clang
libclang-dev
libssl-dev
liblz4-dev
build-essential

**macOS:**
```
brew install lz4
brew install openssl@1.1
```

**Ubuntu:**
```
sudo apt update && sudo apt install -y build-essential libssl-dev pkg-config llvm clang liblz4-dev cmake
```

### Install Rust

ReadySet is written entirely in Rust. If you don’t already have Rust installed, you can install it via rustup (select the version of ‘nightly’ specified in the `rust-toolchain` file):

```curl https://sh.rustup.rs -sSf | sh```
### Runtime Dependencies: Upstream Database and Consul
#### Local Development
To streamline local development, all runtime dependencies (Consul, MySQL, Postgres) can be run with:

```
cp docker-compose.override.yml.example docker-compose.override.yml
docker-compose up -d
```

#### Full Deployment
ReadySet runs alongside a backing MySQL or Postgres database and uses Consul for leader election. You’ll need to pass in your database’s connection string when running the ReadySet server and adapter, and have a Consul instance running.

*Note:* Consul persists data per deployment. Using the same deployment name will carry over any previously installed queries into a new deployment.


## Run ReadySet
### ReadySet Server

First, compile and run ReadySet server.

```
cargo run --bin readyset-server --release -- --upstream-db-url <upstream-url>  --deployment <deployment name>
```

If using the databases supplied by the docker-compose environment in this repository, replace <upstream-url> with the URL of the database corresponding to your database engine:

* MySQL: `mysql://root:readyset@127.1/readyset`
* PostgreSQL: `postgresql://root:readyset@127.1/readyset`

If running with an existing external database, replace <upstream-url> with the connection string for that database.

### ReadySet Adapter

Then, run the adapter binary. The adapter will communicate with servers that have the same deployment name.

**MySQL**
```
cargo run --bin readyset --release -- --upstream-db-url mysql://root:readyset@127.1/readyset  --allow-unauthenticated-connections
  --address 0.0.0.0:3307 --deployment <deployment name>  --prometheus-metrics --query-log-mode all-queries
 ```

**Postgres**
```
cargo run --bin readyset --release -- --upstream-db-url postgresql://postgres:readyset@127.1/readyset  --allow-unauthenticated-connections
  --address 0.0.0.0:5433 --deployment <deployment name> --prometheus-metrics --query-log-mode all-queries
```

The adapter listens for connections at the address specified in the `address` flag.

The `prometheus-metrics` flag exposes an HTTP endpoint in the adapter to allow querying of metrics. This can be reached with an HTTP GET request to <adapter-address>:6034/metrics (e.g., `curl -X GET 127.0.0.1:6034/metrics`).

## Testing

To run tests for the project, run the following command:

```
cargo test --skip integration_serial
```

#### Testing Notes

Certain tests cannot be run in parallel with others, and these tests are typically in files postfixed with _serial. Running the entire set of tests for a package, i.e. `cargo test -p readyset-server` may fail if serial tests are included.

Running tests may require increasing file descriptor limits. You can do so by running `ulimit -Sn 65535`.

## Performance

When running ReadySet in a performance-critical setting, make sure you compile with the `--release` flag.
