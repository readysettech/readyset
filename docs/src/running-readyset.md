# Running and configuring ReadySet

Running ReadySet typically means atleast running:
  1. noria-server
  2. noria-mysql/noria-psql
  3. consul
  4. mysql/postgres

In this section we will document how to run each of them for local testing.

> **NOTE:** This is not how we do things in production.

## Consul + MySQL Docker Stack

Running and testing ReadySet end-to-end locally **requires** a working MySQL instance and an authority,
consul or zookeeper. These can be created for local development via
[`docker-compose`](https://docs.docker.com/compose/), a tool for defining and running
multiple docker applications.

`//docker-compose.yml` and `//docker-compose.override.yml.example` are provided in the
root of the ReadySet repo to create the required dev resources: MySQL and consul.

```
# Set the docker overrides file to open up docker ports locally.
cp docker-compose.override.yml.example docker-compose.override.yml

# Create the docker resources: consul, postgres, zookeeper, and mysql.
docker-compose up -d

# Terminate the docker resources and remove the images.
docker-compose down
```

### Monitoring Stack
`docker-compose` may also be used to run the monitoring resources locally. This
includes:
  * prometheus: A prometheus server that aggregates metrics from a local noria-server and adapter instance.
  * a prometheus push gateway: Used in benchmarks to push prometheus metrics to.
  * grafana: Visualization and graphs!
  * vector: Used to aggregate logs across multiple noria-server instances.
  * node exporter: Used in our deployments to collect system resource utilization.

Each is configured to collect resources from the *default* noria-server and noria-mysql
instance addresses.

> For prometeums metrics to be collected, noria-server and noria-mysql must be run with
> `--prometheus-metrics`. To see prometheus metrics, navigate to `localhost:9090`.

## Running noria-server

Running noria-server can be done with `cargo`.

```
cargo run --bin noria-server -- --replication-url mysql://root:noria@127.0.0.1:3306/test --deployment test-deployment --prometheus-metrics
```

See `--help` for more command-line arguments,  `//noria/server/src/main.rs` for non-public hidden arguments, command-line default arguments.

## Running noria-mysql
Running noria-mysql can also be done with `cargo`. The following command runs a MySQL adapter listening on port `3307`.

```
cargo run --bin noria-mysql -- --upstream-db-url mysql://root:noria@127.0.0.1:3306/test --allow-unauthenticated-connections \
  --address 127.0.0.1:3307 --deployment test-deployment --prometheus-metrics
```

See `--help` for more command-line arguments,  `//noria-client/adapter/src/lib.rs` for non-public hidden arguments, command-line default arguments.

## Running a MySQL client.

```
# Connecting to the MySQL adapter without authentication credentials
mysql -h 127.0.0.1 --port 3307

# Connecting to the upstream database.
mysql -h 127.0.0.1 --port 3306 -uroot -pnoria
```

## Configuring Logging
Both noria-server and noria-mysql output INFO and ERROR logs by default.

Logging can be configured to filter different spans/events, and emit logs
in different formats (json, pretty, full, compact) via command-line arguments
or environment variables. See `//readyset-tracing/src/lib.rs` for a
complete set of options.

During development it can be helpful to enable logs at the `DEBUG` or `TRACE`
log levels.

**Example: Running noria-server with trace level logs and ERROR logs for the `tower` crate:**
```bash
LOG_LEVEL=trace,tower=error cargo run --bin noria-server ...
```

## Common pitfalls

1. Consul persists data per deployment. Using the same deployment name will carry over any previously installed queries (and breakage) into a
   new deployment.

   > **Tip:** Change deployment names when starting a new test.

2. ReadySet behaves in unpredictable ways when the base table state does not match the upstream MySQL database. This can happen if you modify MySQL
   directly via the `mysql` client, for example, by deleting all rows. When in doubt, teardown the MySQL and consul resources with `docker-compose down`
   and bring them back up with `docker-compose up`.
