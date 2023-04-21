<p align="center">
  <img src="https://user-images.githubusercontent.com/38481289/172237414-023c0b04-c597-44b7-8b14-b5b0c382dc07.png" width='70%'>
</p>
<p align="center">
<a href='https://discord.gg/readyset'><img src='https://img.shields.io/badge/Discord-Join%20the%20community-4A154B?style=flat&logo=discord&logoColor=white'></a>
</p>

---

ReadySet is a SQL caching engine designed to help developers enhance the performance and scalability of their existing database-backed applications.

- [What is ReadySet?](#what-is-readyset)
- [Tutorial](#tutorial)
- [Documentation](#documentation)
- [Join the Community](#join-the-community)
- [Development](#development)
- [License](#license)

## What is ReadySet?

ReadySet is a lightweight SQL caching engine that sits between your application and Postgres or MySQL database and turns even the most complex SQL reads into lightning-fast lookups. Unlike other caching solutions, ReadySet keeps the cache up-to-date automatically and requires no changes to your application code.

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

## Tutorial

These instructions show you how to run ReadySet against a new Postgres or MySQL database using Docker. You can follow along in the docs for the [ReadySet Quickstart](https://docs.readyset.io/guides/intro/quickstart/).

### Before you begin

- Install and start [Docker](https://docs.docker.com/engine/install/) for your OS.
- Install the [`psql` shell](https://www.postgresql.org/docs/current/app-psql.html) or the [`mysql` shell](https://dev.mysql.com/doc/refman/8.0/en/mysql.html).

### Step 1. Start the database

Create a Docker container and start the database inside it.

For Postgres:

```bash
docker run -d \
--name=postgres \
--publish=5432:5432 \
-e POSTGRES_PASSWORD=readyset \
-e POSTGRES_DB=testdb \
postgres:14 \
-c wal_level=logical
```

For MySQL:

```bash
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

```bash
curl -O https://raw.githubusercontent.com/readysettech/docs/main/docs/assets/quickstart-data-postgres.sql
```

```bash
PGPASSWORD=readyset psql \
--host=127.0.0.1 \
--port=5432 \
--username=postgres \
--dbname=testdb
```

```
\i quickstart-data-postgres.sql
```

```
\q
```

For MySQL:

```bash
curl -O https://raw.githubusercontent.com/readysettech/docs/main/docs/assets/quickstart-data-mysql.sql
```

```bash
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

```bash
docker run -d \
--name=readyset \
--publish=5433:5433 \
--platform=linux/amd64 \
--volume='readyset:/state' \
--pull=always \
-e DEPLOYMENT_ENV=quickstart_github \
public.ecr.aws/readyset/readyset:beta-2023-04-19 \
--standalone \
--deployment='github-postgres' \
--database-type=postgresql \
--upstream-db-url=postgresql://postgres:readyset@172.17.0.1:5432/testdb \
--address=0.0.0.0:5433 \
--username='postgres' \
--password='readyset' \
--db-dir='/state'
```

For MySQL:

```bash
docker run -d \
--name=readyset \
--publish=3307:3307 \
--platform=linux/amd64 \
--volume='readyset:/state' \
--pull=always \
-e DEPLOYMENT_ENV=quickstart_github \
public.ecr.aws/readyset/readyset:beta-2023-04-19 \
--standalone \
--deployment='github-mysql' \
--database-type=mysql \
--upstream-db-url=mysql://root:readyset@172.17.0.1:3306/testdb \
--address=0.0.0.0:3307 \
--username='root' \
--password='readyset' \
--db-dir='/state'
```

### Next steps

- Connect an existing database

  The tutorial above runs ReadySet against a sample database. To see the power of ReadySet against your own database, swap the `upstream-db-url` argument to point at an existing database for testing.

- Connect your application

  Once you have a ReadySet instance up and running, the next step is to connect your application by swapping out your database connection string to point to ReadySet instead. The specifics of how to do this vary by database client library, ORM, and programming language. See [Connect an App](https://docs.readyset.io/guides/connect/existing-app/) for examples.

  **Note:** By default, ReadySet will proxy all queries to the database, so changing your app to connect to ReadySet should not impact performance. You will explicitly tell ReadySet which queries to cache.

- Cache queries

  Once ReadySet is proxying queries, connect a database SQL shell to ReadySet and use the custom [`SHOW PROXIED QUERIES`](https://docs.readyset.io/guides/cache/cache-queries/#check-query-support) SQL command to view the queries that ReadySet has proxied to your upstream database and identify which queries are supported by ReadySet. Then use the custom [`CREATE CACHE`](https://docs.readyset.io/guides/cache/cache-queries/#cache-queries_1) SQL command to cache supported queries.

  **Note:** To successfully cache the results of a query, ReadySet must support the SQL features and syntax in the query. For more details, see [SQL Support](https://docs.readyset.io/reference/sql-support/).

- Tear down

  When you are done testing, stop and remove the Docker resources:

  ```bash
  docker rm -f readyset postgres mysql \
  && docker volume rm readyset
  ```

- Additional deployment options
  In addition to Docker, you can deploy ReadySet [using binaries](https://docs.readyset.io/releases/readyset-core/). For Kubernetes environments, check out how to [deploy ReadySet to Kubernetes](https://docs.readyset.io/guides/deploy/deploy-readyset-kubernetes/).

## FAQs

**Q: How does ReadySet work under the hood?**

A: The heart of ReadySet is a query engine based on partially-stateful, streaming data flow. To learn more about how it works, see [our docs](https://docs.readyset.io/concepts/overview).

**Q: How does ReadySet keep cached state up to date?**

A: ReadySet receives updates about data changes from your backing database via binlog replication and uses those updates to automatically update its internal state.

**Q: Do I have to send all of my database traffic to ReadySet?**

A: You can if you want to, but it’s not required. You can manually route a subset of your traffic to ReadySet (as you would with a traditional database read replica), or you can send all of it to ReadySet. It's important to note that not all of the queries sent to ReadySet need to be cached– you have fine-grained control over what is cached vs. what is proxied.

**Q: Does ReadySet automatically determine which queries will be cached?**

A: No - by default, ReadySet [proxies queries](https://docs.readyset.io/guides/cache/cache-queries/#check-query-support) to your upstream database so you can profile your application. Only after you run `CREATE CACHE` will ReadySet begin caching a query.

## Documentation

For more information, check out our full docs site [here](http://docs.readyset.io).

## Join the Community

For questions or support, join us on the [ReadySet Community Discord](https://discord.gg/readyset) to chat with our team.

## Development

This section is for developers who want to build ReadySet from source as they work on the ReadySet codebase.

### Install prerequisites

1. Install ReadySet dependencies.

   **macOS with [homebrew](https://brew.sh/):**

   ```bash
   brew install lz4 openssl@1.1 rocksdb
   ```

   Add the following to your [cargo config](https://doc.rust-lang.org/cargo/reference/config.html)
   to make it discover `lz4`:

   ```toml
   [env]
   LIBRARY_PATH = "/opt/homebrew/lib"
   ROCKSDB_LIB_DIR = "/opt/homebrew/lib"
   ```

   **Ubuntu:**

   ```bash
   sudo apt update && sudo apt install -y build-essential libssl-dev pkg-config llvm clang liblz4-dev librocksdb-dev
   sudo apt-get -y install cmake
   ```

   **Arch:**

   ```bash
   sudo pacman -S base-devel clang lz4 rocksdb-static
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
   cargo run --bin readyset --release -- --standalone --database-type=postgresql --upstream-db-url=postgresql://postgres:readyset@127.0.0.1:5432/testdb --username=postgres --password=readyset --address=0.0.0.0:5433 --deployment=<deployment name> --prometheus-metrics --query-log --query-log-ad-hoc
   ```

   **Run against MySQL:**

   ```bash
   cargo run --bin readyset --release -- --standalone --database-type=mysql --upstream-db-url=mysql://root:readyset@127.0.0.1:3306/testdb --username=root --password=readyset --address=0.0.0.0:3307 --deployment=<deployment name> --prometheus-metrics --query-log --query-log-ad-hoc
   ```

   This runs the ReadySet Server and Adapter as a single process, a simple, standard way to run ReadySet that is also the easiest approach when developing. For production deployments, however, it is possible to run the Server and Adapter as separate processes. See the [scale out deployment pattern](https://docs.readyset.io/guides/deploy/production-notes/#scale-out) in the docs.

   For details about ReadySet options, see the CLI docs (coming soon).

1. With ReadySet up and running, you can now connect the Postgres or MySQL shell:

   **Postgres:**

   ```bash
   PGPASSWORD=readyset psql
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

### Performance

When running ReadySet in a performance-critical setting, make sure you compile with the `--release` flag.

**Note:** This repository contains a snapshot of the current state of the
ReadySet internal repository. Our team is currently working on getting our live
codebase, including all development history, ready to be hosted on GitHub, at
which point we'll force push to this repository.

## License

ReadySet is licensed under the BSL 1.1 license, converting to the open-source Apache 2.0 license after 4 years. It is free to use on any number of nodes.
