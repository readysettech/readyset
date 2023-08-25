<p align="center">
  <img src="https://user-images.githubusercontent.com/38481289/172237414-023c0b04-c597-44b7-8b14-b5b0c382dc07.png" width='40%'>
</p>

---

Lightning fast SQL cache without code changes. 

## What is ReadySet?

ReadySet is a lightweight query cache that sits between your application and database and turns even the most complex SQL reads into lightning-fast lookups. Unlike other caching solutions, ReadySet keeps the cache in sync with your database automatically and requires no changes to your application code.

<br>
<p align="center">
  <img src='https://user-images.githubusercontent.com/38481289/172237407-e0546ef3-2095-49ab-be82-a177e507c6d1.png' width='70%'>
</p>
<br>

Based on years of dataflow research at [MIT](https://pdos.csail.mit.edu/papers/noria:osdi18.pdf), ReadySet stores the results of queries in-memory and automatically keeps these results up-to-date as the underlying database changes. ReadySet can do this automatically because it listens to your database’s replication stream.

This means:

- No extra code to keep your cache and database in sync
- No extra code to evict stale records
- No TTLs to set - your cache is as up-to-date as your replication lag

ReadySet is wire-compatible with MySQL and Postgres.

### Quick start
Curious to see how ReadySet works? Run through our [quick start](https://docs.readyset.io/quickstart) to kick the tires and cache queries in under five minutes. 

---
### Install with Docker
ReadySet is a cache that sits between an application and a database. Getting up and running requires that you do three things: download ReadySet, connect it to a database, and create a cache. Then you're off to the races. 

#### 1. Download
The easiest way to install ReadySet is via Docker. First, download our Docker Compose file:

```
curl -L -o compose.yml "https://readyset.io/quickstart/compose.yml"
```

#### 2. Point to a database

Make sure your database is [configured to run with ReadySet](https://docs.readyset.io/deploy/configure-your-database) and then modify the downloaded Docker Compose file to include your database connection string:

```
name: readyset
services:
  cache:
    ...
    environment:
      # UPSTREAM_DB_URL: <your DB connection string>
  ...
  grafana:
    ...
    environment:
      # UPSTREAM_DB_URL: <your DB connection string>
```

#### 3. Run ReadySet
```
docker compose up -d
```

#### 4. Configure caching 

Once ReadySet is up and running, you'll need to [create caches](https://docs.readyset.io/cache/creating-a-cache) for the queries you want to speed up. Check out our [caching guide](https://docs.readyset.io/cache/profiling-queries) for details. 

## Documentation

For more information, check out our [documentation](https://docs.readyset.io).

## Join the Community

For questions or support, join us on the [ReadySet Community Slack](https://join.slack.com/t/readysetcommunity/shared_invite/zt-1c7bxdxo7-Y6KuoLfc1YWagLk3xHSrsw) to chat with our team.

---
### Contribute
We welcome contributions as [GitHub pull requests](https://github.com/readysettech/readyset/pulls), creating [Issues](https://github.com/readysettech/readyset/issues), advocacy, and particpating in our [community](#join-the-community)! 

### Building from source
See our [instructions](./community-development.md) on how to build ReadySet from source.

---
## FAQs

**Q: How does ReadySet work under the hood?**

A: The heart of ReadySet is a query engine based on partially-stateful, streaming data flow. To learn more about how it works, see [our docs](https://docs.readyset.io/concepts/overview).

**Q: How does ReadySet keep cached state up to date?**

A: ReadySet receives updates about data changes from your backing database via binlog replication and uses those updates to automatically update its internal state.

**Q: Do I have to send all of my database traffic to ReadySet?**

A: You can if you want to, but it’s not required. You can manually route a subset of your traffic to ReadySet (as you would with a traditional database read replica), or you can send all of it to ReadySet. It's important to note that not all of the queries sent to ReadySet need to be cached– you have fine-grained control over what is cached vs. what is proxied.

**Q: Does ReadySet automatically determine which queries will be cached?**

A: No - by default, ReadySet proxies queries to your upstream database so you can profile your application. Only after you run [`CREATE CACHE FROM <query>`](https://docs.readyset.io/cache/creating-a-cache) will ReadySet begin caching a query.

## License

ReadySet is licensed under the BSL 1.1 license, converting to the open-source Apache 2.0 license after 4 years. It is free to use on any number of nodes.
