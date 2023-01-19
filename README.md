<p align="center">
  <img src="https://user-images.githubusercontent.com/38481289/172237414-023c0b04-c597-44b7-8b14-b5b0c382dc07.png" width='70%'>
</p>
<p align="center">
<a href='https://discord.gg/readyset'><img src='https://img.shields.io/badge/Discord-Join%20the%20community-4A154B?style=flat&logo=discord&logoColor=white'></a>
<a href='https://readysettech.typeform.com/to/BqovNk8A?typeform-source=github.com'><img src='https://img.shields.io/badge/ReadySet-Join%20the%20cloud%20waitlist-7648B5?logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAADrUlEQVRIS41V6UsVURQ/d2Z8vkVsoQ0iKsMgyd4rhGghogiqL0VQaUGLQQvtVPihsJ20KEgrxchK2vsg9CkikugPCNqQsiKwIqISKyzfmzmdu83ceU+rB8PcN3Pn/M75/X7nXAb/+HVsf78crMwNh3lg0+UwV909iDBYXVBb0vK3EKy/l483f/Zsy2W2xYMGgbNBbAJ0aE/iWLLPWDkPby1Fe9TQroyZMQ8gAhGQJcDClVAi4ln8SzrCmsrSZtIhgLZNvxdbmG7lWQaZyo9lJZIe+S4A1Wu+J2phuXM0dVOD+ABts9FJF/em+Sadbfa9tG44fHv0A761flVACtxMiL5vp0rKVCU+wJ11HvIMdbZBcJl52ZnBfuUvd7ynigKxw5WFNREAFzd7GCfm4i4qgDAVOoAGebH9IwnrQvGJkcAcBp27XyvKQtRhoiZpCYCzOwggA5AgkFhGgsw/74iMH276meUiWWXJ8aFgRy2/qs7dbwwLyz2FtaWMHazOVMR7rGsyOIH00t0FmLAEYdxCCx5s/JWjSeFYC8bvGugHRw/h3Z53ucLb7ipWtV/RQwAmSJRA+tNk6ukBRI2M7/UidFSRJkD0CIOETcK2HkHiH0FooC+qRKzp7outvF5WHQMnziAvweBtczf8fNbTp4W1bqyyFpFn7oNoLYyKNMjkLQ7kxVEEf3Lku19hqn6IqObLvW7outtlCO4CqziFGFPBOEjNAenchp0oNImTJtxdE8sBIgl+ITxv+BXiO2ThnZ0+AKeLLao3AYxKDMqSMxnEYwj5BQivbmb67AEO8nTbJ6VD0IBsXiPXQHJuVqKfjR4GMJwYKIgCfLhvatJXx3OhdQPKO5vVhCsjHlyROoRBEkRR0QiAKD3//ka6LKr6RHZ80M2BGQgEZDcz210vCJ9+QVYhbKqAEqRH3V4Gh/eRFlwH5SptZR5wQVYzZs+uSfUjmABItaBHAZgGCVUihJY21sElGAm/AmDMnOxm1FPXhSlnhkkAAXKZqlD+593M14EmUvzqowwadxFNHFC5S4xu3mBEiz6ceCXTzg0SsYPz4AA6qSJIa3HDmsgKuIVPVinKjNkVgEiR2732yIbscc3RJlzF8vwMXDf5DnW40fHh2YXBIWR7a+c2xi5pZnLP0Vtop3og49P1Fwsn0kxMXzkgEXq6rfxltxn9C379HvrJFj1CAncJ2gzR5Vq8xzVnrWB2/w+A3jOjGVeTTS/5mqiRLmYXepX7DzkXzYyz138Ac/pGks+x6G0AAAAASUVORK5CYII='></a>
</p>

---

ReadySet is a SQL caching engine designed to help developers enhance the performance and scalability of their existing database-backed applications.

- [What is ReadySet?](#what-is-readyset)
- [Quickstart](#quickstart)
- [Documentation](#documentation)
- [Join the Community](#join-the-community)
- [Development](#development)
- [License](#license)

## What is ReadySet?

ReadySet is a lightweight SQL caching engine that sits between your application and database and turns even the most complex SQL reads into lightning-fast lookups. Unlike other caching solutions, ReadySet keeps the cache up-to-date automatically and requires no changes to your application code.

<br>
<p align="center">
  <img src='https://user-images.githubusercontent.com/38481289/172237407-e0546ef3-2095-49ab-be82-a177e507c6d1.png' width='70%'>
</p>
<br>

Based on years of dataflow research at MIT[^1], ReadySet stores the results of queries in-memory and automatically keeps these results up-to-date as the underlying database changes. ReadySet can do this automatically because it listens to your database's replication stream.

This means:

- No extra code to keep your cache and database in sync
- No extra code to evict stale records
- No TTLs to set - your cache is as up-to-date as your replication lag

ReadySet is wire-compatible with MySQL and Postgres.

[^1]: See the [Noria](https://pdos.csail.mit.edu/papers/noria:osdi18.pdf) paper.

## Quickstart

These instruction show you how to run ReadySet against a new Postgres or MySQL database in Docker. To use an existing database, see the [Quickstart page](https://docs.readyset.io/guides/quickstart/#use-an-existing-database) in our docs. For a full walk-through of ReadySet's capabilities and features, see the [ReadySet Tutorial](https://docs.readyset.io/guides/tutorial/).

### Before you begin

- Install and start [Docker](https://docs.docker.com/engine/install/) for your OS.
- Install the [`psql` shell](https://www.postgresql.org/docs/current/app-psql.html) or the [`mysql` shell](https://dev.mysql.com/doc/refman/8.0/en/mysql.html).

### Step 1. Start the database

Create a Docker container and start the database inside it.

For Postgres:

```
docker run -d \
--name=postgres \
--publish=5432:5432 \
-e POSTGRES_PASSWORD=readyset \
-e POSTGRES_DB=testdb \
postgres:14 \
-c wal_level=logical
```

For MySQL:

```
docker run -d \
--name=mysql \
--publish=3306:3306 \
-e MYSQL_ROOT_PASSWORD=readyset \
-e MYSQL_DATABASE=testdb \
mysql
```

### Step 2. Load sample data

Download a sample data file and load it into the database via the SQL shell.

For Postgres:

```
curl -O https://raw.githubusercontent.com/readysettech/docs/main/docs/assets/quickstart-data-postgres.sql
```

```
PGPASSWORD=readyset psql \
--host=127.0.0.1 \
--port=5432 \
--username=postgres \
--dbname=testdb
```

``` sh
\i quickstart-data-postgres.sql
```

```
\q
```

For MySQL:

```
curl -O https://raw.githubusercontent.com/readysettech/docs/main/docs/assets/quickstart-data-mysql.sql
```

```
mysql \
--host=127.0.0.1 \
--port=3306 \
--user=root \
--password=readyset \
--database=testdb
```

```
source quickstart-data-mysql.sql;
```

```
\q
```

### Step 3. Start ReadySet

Create a Docker container and start ReadySet inside it, connecting ReadySet to the database via the connection string in `--upstream-db-url`:

For Postgres:

```
docker run -d \
--name=readyset \
--publish=5433:5433 \
--platform=linux/amd64 \
--volume='readyset:/state' \
--pull=always \
-e DEPLOYMENT_ENV=quickstart_github \
public.ecr.aws/readyset/readyset:beta-2022-12-15 \
--standalone \
--deployment='github-postgres' \
--database-type=postgresql \
--upstream-db-url=postgresql://postgres:readyset@172.17.0.1:5432/testdb \
--address=0.0.0.0:5433 \
--username='postgres' \
--password='readyset' \
--query-caching='explicit' \
--db-dir='/state'
```

For MySQL:

```
docker run -d \
--name=readyset \
--publish=3307:3307 \
--platform=linux/amd64 \
--volume='readyset:/state' \
--pull=always \
-e DEPLOYMENT_ENV=quickstart_github \
public.ecr.aws/readyset/readyset:beta-2022-12-15 \
--standalone \
--deployment='github-mysql' \
--database-type=mysql \
--upstream-db-url=mysql://root:readyset@172.17.0.1:3306/testdb \
--address=0.0.0.0:5433 \
--username='root' \
--password='readyset' \
--query-caching='explicit' \
--db-dir='/state'
```

### Next steps

- Connect an app

    Once you have a ReadySet instance up and running, the next step is to connect your application by swapping out your database connection string to point to ReadySet instead. The specifics of how to do this vary by database client library, ORM, and programming language. See [Connect an App](https://docs.readyset.io/guides/connect-an-app/) for examples.

    **Note:** By default, ReadySet will proxy all queries to the database, so changing your app to connect to ReadySet should not impact performance. You will explicitly tell ReadySet which queries to cache.   

- Cache queries

    Once you are running queries against ReadySet, connect a database SQL shell to ReadySet and use the custom [`SHOW PROXIED QUERIES`](https://docs.readyset.io/guides/cache-queries/#identify-queries-to-cache) SQL command to view the queries that ReadySet has proxied to your upstream database and identify which queries are supported by ReadySet. Then use the custom [`CREATE CACHE`](https://docs.readyset.io/guides/cache-queries/#cache-queries_1) SQL command to cache supported queries.

    **Note:** To successfully cache the results of a query, ReadySet must support the SQL features and syntax in the query. For more details, see [SQL Support](https://docs.readyset.io/reference/sql-support/).

- Tear down

    When you are done testing, stop and remove the Docker resources:

    ```
    docker rm -f readyset postgres mysql \
    && docker volume rm readyset
    ```

- Deploy to production

    To run ReadySet in a cloud environment, see the [Deploy with ReadySet Cloud](https://docs.readyset.io/guides/deploy-readyset-cloud/) and [Deploy with Kubernetes](https://docs.readyset.io/guides/deploy-readyset-kubernetes/) pages in our docs. You can also run ReadySet yourself using [binaries](https://docs.readyset.io/releases/readyset-core/).


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

For questions or support, join us on the [ReadySet Community Discord](https://discord.gg/readyset), post questions on our [Github forum](https://github.com/readysettech/readyset/discussions), or schedule an [office hours chat](https://calendly.com/d/d5n-y44-mbg/office-hours-with-ready-set) with our team.

Everyone is welcome!

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
cargo run --bin readyset-server --release -- --upstream-db-url <upstream-url>  --deployment <deployment name>
```

If using the databases supplied by the docker-compose environment in this repository, replace <upstream-url> with the URL of the database corresponding to your database engine:

* MySQL: `mysql://root:readyset@127.1/readyset`
* PostgreSQL: `postgresql://root:readyset@127.1/readyset`

If running with an existing external database, replace <upstream-url> with the connection string for that database.

#### ReadySet Adapter

Then, run the adapter binary. The adapter will communicate with servers that have the same deployment name.

**MySQL**
```
cargo run --bin readyset --release -- --database-type mysql --upstream-db-url mysql://root:readyset@127.1/readyset  --allow-unauthenticated-connections
  --address 0.0.0.0:3307 --deployment <deployment name>  --prometheus-metrics --query-log --query-log-ad-hoc
 ```

**Postgres**
```
cargo run --bin readyset --release -- --database-type postgresql --upstream-db-url postgresql://postgres:readyset@127.1/readyset  --allow-unauthenticated-connections
  --address 0.0.0.0:5433 --deployment <deployment name> --prometheus-metrics --query-log --query-log-ad-hoc
```

The adapter listens for connections at the address specified in the `address` flag.

The `query-log` and `query-log-ad-hoc` flags ensure that queries are sent to the Prometheus client running in the adapter.

The `prometheus-metrics` flag exposes an HTTP endpoint in the adapter to allow querying of metrics. This can be reached with an HTTP GET request to <adapter-address>:6034/metrics, `curl -X GET 127.0.0.1:6034/metrics`).

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
