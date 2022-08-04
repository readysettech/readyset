<p align="center">
  <img src="https://user-images.githubusercontent.com/38481289/172237414-023c0b04-c597-44b7-8b14-b5b0c382dc07.png" width='70%'>
</p>
<p align="center">
<a href='https://join.slack.com/t/readysetcommunity/shared_invite/zt-1c7bxdxo7-Y6KuoLfc1YWagLk3xHSrsw'><img src='https://img.shields.io/badge/Slack-Join%20the%20community-4A154B?style=flat&logo=slack&logoColor=white'></a>
<a href='https://readysettech.typeform.com/to/BqovNk8A?typeform-source=github.com'><img src='https://img.shields.io/badge/ReadySet-Join%20the%20cloud%20waitlist-7648B5?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAADrUlEQVRIS41V6UsVURQ/d2Z8vkVsoQ0iKsMgyd4rhGghogiqL0VQaUGLQQvtVPihsJ20KEgrxchK2vsg9CkikugPCNqQsiKwIqISKyzfmzmdu83ceU+rB8PcN3Pn/M75/X7nXAb/+HVsf78crMwNh3lg0+UwV909iDBYXVBb0vK3EKy/l483f/Zsy2W2xYMGgbNBbAJ0aE/iWLLPWDkPby1Fe9TQroyZMQ8gAhGQJcDClVAi4ln8SzrCmsrSZtIhgLZNvxdbmG7lWQaZyo9lJZIe+S4A1Wu+J2phuXM0dVOD+ABts9FJF/em+Sadbfa9tG44fHv0A761flVACtxMiL5vp0rKVCU+wJ11HvIMdbZBcJl52ZnBfuUvd7ynigKxw5WFNREAFzd7GCfm4i4qgDAVOoAGebH9IwnrQvGJkcAcBp27XyvKQtRhoiZpCYCzOwggA5AgkFhGgsw/74iMH276meUiWWXJ8aFgRy2/qs7dbwwLyz2FtaWMHazOVMR7rGsyOIH00t0FmLAEYdxCCx5s/JWjSeFYC8bvGugHRw/h3Z53ucLb7ipWtV/RQwAmSJRA+tNk6ukBRI2M7/UidFSRJkD0CIOETcK2HkHiH0FooC+qRKzp7outvF5WHQMnziAvweBtczf8fNbTp4W1bqyyFpFn7oNoLYyKNMjkLQ7kxVEEf3Lku19hqn6IqObLvW7outtlCO4CqziFGFPBOEjNAenchp0oNImTJtxdE8sBIgl+ITxv+BXiO2ThnZ0+AKeLLao3AYxKDMqSMxnEYwj5BQivbmb67AEO8nTbJ6VD0IBsXiPXQHJuVqKfjR4GMJwYKIgCfLhvatJXx3OhdQPKO5vVhCsjHlyROoRBEkRR0QiAKD3//ka6LKr6RHZ80M2BGQgEZDcz210vCJ9+QVYhbKqAEqRH3V4Gh/eRFlwH5SptZR5wQVYzZs+uSfUjmABItaBHAZgGCVUihJY21sElGAm/AmDMnOxm1FPXhSlnhkkAAXKZqlD+593M14EmUvzqowwadxFNHFC5S4xu3mBEiz6ceCXTzg0SsYPz4AA6qSJIa3HDmsgKuIVPVinKjNkVgEiR2732yIbscc3RJlzF8vwMXDf5DnW40fHh2YXBIWR7a+c2xi5pZnLP0Vtop3og49P1Fwsn0kxMXzkgEXq6rfxltxn9C379HvrJFj1CAncJ2gzR5Vq8xzVnrWB2/w+A3jOjGVeTTS/5mqiRLmYXepX7DzkXzYyz138Ac/pGks+x6G0AAAAASUVORK5CYII='></a>
</p>

---

ReadySet is a SQL caching engine designed to help developers enhance the performance and scalability of their existing database-backed applications.

- [What is ReadySet?](#what-is-readyset)
- [Getting Started](#getting-started)
- [Documentation](#documentation)
- [Join the Community](#join-the-community)
- [Development](#development)
- [License](#license)

## What is ReadySet?

ReadySet is a lightweight SQL caching engine that precomputes frequently-accessed query results and automatically keeps these results up-to-date over time as the underlying data in your database changes. ReadySet is wire-compatible with MySQL and Postgres and can be adopted without code changes.

<br>
<p align="center">
  <img src='https://user-images.githubusercontent.com/38481289/172237407-e0546ef3-2095-49ab-be82-a177e507c6d1.png' width='70%'>
</p>
<br>

ReadySet acts as both a SQL cache and a proxy– when you first connect ReadySet to your application, it defaults to proxying all of your queries to your backing database, so it doesn't change the behavior of your application.

From there, you can monitor the performance of your queries using the ReadySet dashboard, and cache a subset of them in ReadySet. ReadySet automatically keeps cached results up-to-date over time as the underlying data in your database changes, so there's no need to write cache maintenance logic.


## Getting Started

### Deploy ReadySet
The ReadySet orchestrator is a command line tool that uses Docker Compose to spin up a ReadySet instance on your local machine. The following command downloads and runs the ReadySet orchestrator:

```
bash -c "$(curl -sSL https://launch.readyset.io)"
```
The orchestrator will prompt you to select a wire protocol, and to connect ReadySet to an existing database (or create a new one).

### Connect to ReadySet

- **To connect to ReadySet via the MySQL or Postgres command line**, run the command that the orchestrator prints after successfully deploying a ReadySet instance.
- **To connect ReadySet to your application**, use the ReadySet connection string (printed by the orchestrator after successfully deploying ReadySet) in the same way you would if it were a MySQL or Postgres connection string. Check out our [docs](https://docs.readyset.io/connecting) for more information.

Once you connect to ReadySet, you can start issuing queries as you would with a traditional database.

### Monitor Query Performance

By default, all queries sent to ReadySet are proxied to the backing database. You can see the current list of proxied queries and their performance profiles via the ReadySet dashboard which is accessible on `http://localhost:4000`.


### Cache Queries
ReadySet supports caching both prepared statements (i.e. parameterized queries) and one-off queries. To cache a query in ReadySet, you’ll need either the query ID (which can be found in the dashboard, or by running `SHOW CACHES`) or the full query text (i.e. the `SELECT` statement).

From there, you can run `CREATE CACHE FROM <query ID>` or `CREATE CACHE FROM <select statement>` via the MySQL or Postgres client.

#### Write Handling
You can either send writes to ReadySet or directly to your backing database. If you send ReadySet a write, it will be proxied to your backing database.  ReadySet waits to receive updates from your database's bin logs before updating the cached state to reflect those writes.

**Note:** The ReadySet Orchestrator is updated frequently with releases built off our internal ReadySet repository. Because of this, the orchestrator may have features not yet in our public repo. We're working on making our live codebase available on Github.

## FAQs
**Q: How does ReadySet work under the hood?**

A: The heart of ReadySet is a query engine based on partially-stateful, streaming data flow. To learn more about how it works, see [our docs](https://docs.readyset.io/concepts/overview).

**Q: How does ReadySet keep cached state up to date?**

A: ReadySet receives updates about data changes from your backing database via binlog replication and uses those updates to automatically update its internal state.

**Q: Do I have to send all of my database traffic to ReadySet?**

A: You can if you want to, but it’s not required. You can manually route a subset of your traffic to ReadySet (as you would with a traditional database read replica), or you can send all of it to ReadySet. It's important to note that not all of the queries sent to ReadySet need to be cached– you have fine-grained control over what is cached vs. what is proxied.

## Documentation
For more information, check out our full docs site [here](http://docs.readyset.io).

## Join the Community

For questions or support, join us on the [ReadySet Community Slack](https://join.slack.com/t/readysetcommunity/shared_invite/zt-18z5bxv02-C5R08CYFL00jXmIqjd7Ziw). You can also post questions on our [Github forum](https://github.com/readysettech/readyset/discussions). Everyone is welcome!

## Development

### Prerequisites

#### Install Dependencies

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
#### Install Rust
ReadySet is written entirely in Rust. If you don’t already have Rust installed, you can install it via rustup (select the version of ‘nightly’ specified in the `rust-toolchain` file):

```curl https://sh.rustup.rs -sSf | sh```
#### Runtime Dependencies: Upstream Database and Consul
##### Local Development
To streamline local development, all runtime dependencies (Consul, MySQL, Postgres) can be run with:

```
cp docker-compose.override.yml.example docker-compose.override.yml
docker-compose up -d
```

##### Full Deployment
ReadySet runs alongside a backing MySQL or Postgres database and uses Consul for leader election. You’ll need to pass in your database’s connection string when running the ReadySet server and adapter, and have a Consul instance running.

*Note:* Consul persists data per deployment. Using the same deployment name will carry over any previously installed queries into a new deployment.


### Run ReadySet
#### ReadySet Server

First, compile and run ReadySet server.

```
cargo run --bin readyset-server --features display_literals --release -- --replication-url <upstream-url>  --deployment <deployment name>
```

If using the databases supplied by the docker-compose environment in this repository, replace <upstream-url> with the URL of the database corresponding to your database engine:

* MySQL: `mysql://root:readyset@127.1/readyset`
* PostgreSQL: `postgresql://root:readyset@127.1/readyset`

If running with an existing external database, replace <upstream-url> with the connection string for that database.

#### ReadySet MySQL or Postgres Adapter

Then, run the adapter binary corresponding to your upstream database (MySQL or Postgres) The adapter will communicate with servers that have the same deployment name.

**MySQL**
```
cargo run --bin readyset-mysql --features display_literals --release -- --upstream-db-url mysql://root:readyset@127.1/readyset  --allow-unauthenticated-connections
  --address 0.0.0.0:3307 --deployment <deployment name>  --prometheus-metrics --query-log --query-log-ad-hoc
 ```

**Postgres**
```
cargo run --bin readyset-psql --features display_literals --release -- --upstream-db-url postgresql://postgres:readyset@127.1/readyset  --allow-unauthenticated-connections
  --address 0.0.0.0:5433 --deployment <deployment name> --prometheus-metrics --query-log --query-log-ad-hoc
```

The adapter listens for connections at the address specified in the `address` flag.

The `query-log` and `query-log-ad-hoc` flags ensure that queries are sent to the Prometheus client running in the adapter.

The `prometheus-metrics` flag exposes an HTTP endpoint in the adapter to allow querying of metrics. This can be reached with an HTTP GET request to <adapter-address>:6034/prometheus (e.g., `curl -X GET 127.0.0.1:6034/prometheus`).

### Testing

To run tests for the project, run the following command:

```
cargo test --skip integration_serial
```
##### Testing Notes

Certain tests cannot be run in parallel with others, and these tests are typically in files postfixed with _serial. Running the entire set of tests for a package, i.e. `cargo test -p readyset-server` may fail if serial tests are included.

Running tests may require increasing file descriptor limits. You can do so by running `ulimit -Sn 65535`.

### Performance

When running ReadySet in a performance-critical setting, make sure you compile with the `--release` flag.

**Note:** This repository contains a snapshot of the current state of the
ReadySet internal repository. Our team is currently working on getting our live
codebase, including all development history, ready to be hosted on GitHub, at
which point we'll force push to this repository.

## License

ReadySet is licensed under the BSL 1.1 license, converting to the open-source Apache 2.0 license after 4 years. It is free to use on any number of nodes. The ReadySet team is hard at work getting the codebase ready to be hosted on Github.

ReadySet is also available as a paid cloud service that can be deployed to your accounts or hosted entirely by ReadySet. You can get early access to our cloud product by signing up [here](https://readysettech.typeform.com/to/BqovNk8A).
