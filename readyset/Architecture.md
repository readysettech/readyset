# Architecture of Readyset
Readyset takes relational database queries and uses **Readyset Adapter** to identify cache supported queries and passes parsed queries to **Readyset Server** which caches results and listens for updates in **Upstream Databases** MySQL and Postgres.

## Diagram
![Readyset Arcitecture](/readyset/assets/Readyset_Architecture.jpeg)

## Application Client
1. The Application sends SQL queries to the ReadySet Adapter, abstracting database, so the connection is indistinguishable from connecting directly to database.

2. There are many supported [SQL queries](https://readyset.io/docs/reference/features/queries) like 'SELECT', 'DISTINCT', 'JOIN', etc.

## Readyset Adapter

It uses nom-sql library to parse SQL queries from application to an Abstract Syntax Tree.

Readyset Adapter acts as a proxy, i.e, for supported queries it checks with the Readyset Server and for write, transactional and unsupported queries, it proxies directly to upstream database.

## Readyset Server
The ReadySet Server caches results and listens for changes in the database using Change Data Capture (CDC).

It uses Noria as the caching engine and maintain core dataflow. It receives parsed queries from the Adapter and uses dataflow graphs to materialize and update caches results. It synchronizes cache by updating from CDC on upstream DB.

## Upstream Database
This is the primary data source and executes all the proxied queries.

**Readyset acts an an intermediary between client and database allowing faster reads for repetitive cached queries**
