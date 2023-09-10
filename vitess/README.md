# Vitess Support for ReadySet

This directory supports a set of helper scripts and documentation related Vitess support for ReadySet.

## Background

Vitess is a database clustering system for horizontal scaling of MySQL. It was created by YouTube and is now a CNCF project. It is used by a large number of companies including Slack, Square, Shopify, PlanetScale, GitHub and many others.

Architecturally, Vitess is a middleware layer that sits between MySQL and the application. It provides a number of features including connection pooling, sharding, and resharding. It also provides a number of tools for managing the cluster including a web-based UI, a command line tool, and a REST API.

Unfortunately for ReadySet (which has pure MySQL support), Vitess is not a drop-in replacement for MySQL for replication-based services (it does not expose the standard MySQL replication protocol). However, it is possible to receive the change stream from Vitess using the VStream gRPC API.

## Implementation Details

At the highest level, Vitess support for ReadySet consists of a few key components:

* `VitessReplicator` - A ReadySet `Replicator` implementation that performs the initial snapshot of the Vitess database using (as of Sep 10, 2023) a slightly modified logic from the regular MySQL replicator. The major difference is that we only snapshot the schema, not the data. This is because we will receive the data via the VStream API during the initial VStream Copy phase of data replication.

* `VitessConnector` - A ReadySet `Connector` implementation that uses the VStream API to receive the change stream from Vitess, including the initial VStream Copy phase that sends the data from all tables to the connector.

* TODO: Add more details here.

On a lower level, the implementation is based on the following components:

* [Vitess gRPC client](https://github.com/kovyrin/vitess-grpc-rust) used to interact with the VStream API.
* The `readyset-vitess-data` crate (vendored in this repository) that contains the Vitess-specific data structures parsing logic along with MySQL GTID parsing logic.


### Configuration

The Vitess support for ReadySet is configured via the `--upstream-db-url` command line argument to readyset. The URL is in the following format:

```
vitess://<vtgate-host>:<vtgate-port>/<keyspace>?mysql_port=<mysql-port>
```

The `vtgate-host` and `vtgate-port` are used to connect to VStream API. The `keyspace` is the name of the keyspace to replicate. The `mysql-port` is the MySQL protocol port on VTgate that is used for the initial snapshot process and to forward user queries to the underlying MySQL instances.

## Usage

TODO: Explain how to run ReadySet with Vitess support.

To be explained:

* Running vitess locally for development and testing purposes.
* Running ReadySet with Vitess support on top of a given Vitess cluster.
