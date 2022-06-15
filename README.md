
<p align="center">
  <img src="https://user-images.githubusercontent.com/38481289/172237414-023c0b04-c597-44b7-8b14-b5b0c382dc07.png" width='70%'>
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
For instructions on how to build ReadySet from source, see our [developer quick start guide](development.md).

**Note:** This repository contains a snapshot of the current state of the
ReadySet internal repository. Our team is currently working on getting our live
codebase, including all development history, ready to be hosted on GitHub, at
which point we'll force push to this repository.

## License

ReadySet is licensed under the BSL 1.1 license, converting to the open-source Apache 2.0 license after 4 years. The ReadySet team is hard at work getting the codebase ready to be hosted on Github.

ReadySet is also available as a paid cloud service that can be deployed to your accounts or hosted entirely by ReadySet. You can get early access to our cloud product by signing up [here](https://readysettech.typeform.com/to/BqovNk8A).
