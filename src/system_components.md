# System Components

## Noria
Noria accepts a set of parameterized SQL queries (think [prepared
statements](https://en.wikipedia.org/wiki/Prepared_statement)), and produces a [data-flow
program](https://en.wikipedia.org/wiki/Stream_processing) that maintains [materialized
views](https://en.wikipedia.org/wiki/Materialized_view) for the output of those queries. Reads
now become fast lookups directly into these materialized views, as if the value had been
directly read from a cache (like memcached). The views are automatically kept up-to-date by
Noria through the data-flow.

### Client library 
`noria/noria` in the `readyset` repo.  Provides bindings that allow the applications to issue requests to Noria (both writes and reads). The database adapters use the Noria client library under the hood to interact with Noria, but when requests are not encoded in the MySQL or Postgres binary protocols, then users can rely on the Noria client library directly, and thus save time on this translation. The trade off here is that users have to rewrite parts of their application to use the client library. This is fairly light weight in practice, but it isn't quite as "plug-and-play" as using the database adapters. 

### Dataflow engine 

`noria/server` in the `readyset` repo. Implements the core dataflow engine that receives prepared statements from the Noria client and constructs dataflow representation of these queries. The dataflow engine can be distributed (i.e. through sharding)– each node runs either a worker or both a worker (`noria/server/source/worker/`) and a controller (`noria/server/source/controller`). There is only one controller running at a time (elected through Zookeeper).

#### Controller 
The controller ("the control plane") is responsible for planning the dataflow graph based on queries it has received (i.e. deciding which nodes go in which domain, as well as which worker is responsible for running that domain), but it doesn't perform any of the actual data processing itself. The controller is also responsible for detecting any worker failures. 

#### Workers 
Workers ("the data plane") run the different components of the dataflow graph (e.g. join, aggregate, projection nodes). Dataflow subgraphs are grouped together into logical units called "domains" that are run on a single worker (for pipelining). 

#### Persistent base table storage in RocksDB
Base tables are stored in RocksDB by default. This feature can be turned off, and likely *should* be turned off for most clients who are connecting ReadySet to their existing database. Otherwise, ReadySet materializes intermediate views that are composed to construct the final query results entirely in memory. 

## Database adapters 
`noria-mysql` and `noria-psql` in the `readyset` repo, referring to the MySQL and Postgres database adapters, respectively. 
- Using a database adapter is optional– in general, you can interact with Noria through a database adapter, or by using the Noria client library directly.  The database adapter makes Noria "look" like an existing MySQL or Postgres database, and allows users to continue using MySQL/Postgres ORMs or client libraries.
- The adapter can be thought of as a "shim" that wraps the core Noria client library. Read requests (encoded in MySQL/Postgres binary protocols) are sent directly to the adapter. The adapter translates from binary protocol to a Noria query, sends this query to the noria client (`noria/noria`, see below) which resolves it and sends a response back to the adapter. The adapter then forwards the received query results back to the user.



