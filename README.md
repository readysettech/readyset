<p align="center">
  <img src="https://user-images.githubusercontent.com/38481289/172237414-023c0b04-c597-44b7-8b14-b5b0c382dc07.png" width='40%'>
</p>

---
[![Build status](https://badge.buildkite.com/76e02771ab1f0706b7840f47c5fed0e315a56c408d86c0de8c.svg?branch=main)](https://buildkite.com/readyset/readyset-public)
[![Slack](https://img.shields.io/badge/Join%20the%20community-purple?logo=slack&logoColor=white)](https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw)
![Rust](https://img.shields.io/badge/Built%20with%20Rust-grey?logo=rust&logoColor=white)

ReadySet is a lightweight query cache that sits between your application and database and turns even the most complex SQL reads into lightning-fast lookups.


Unlike other caching solutions, ReadySet keeps the cache in sync with your database automatically and requires no changes to your application code.

<br>
<p align="center">
  <img src='https://user-images.githubusercontent.com/38481289/172237407-e0546ef3-2095-49ab-be82-a177e507c6d1.png' width='70%'>
</p>
<br>

This means:

- No extra code to keep your cache and database in sync
- No extra code to evict stale records
- No TTLs to set - your cache is as up-to-date as your replication lag

ReadySet is wire-compatible with Postgres and MySQL.

---
### Demo
Curious to see how ReadySet works? Run through our [demo](https://docs.readyset.io/quickstart) to kick the tires and cache queries in under five minutes.

---
### Install with Docker
Getting up and running with ReadySet requires that you do three things: download ReadySet, connect it to a database, and create a cache.

#### 1. Download
The easiest way to install ReadySet is via Docker. First, download our Docker Compose file:

```
curl -L -o compose.yml "https://readyset.io/quickstart/compose.yml"
```

#### 2. Point to a database

Make sure your database is [configured to run with ReadySet](https://docs.readyset.io/get-started/configure-your-database) and then modify the downloaded Docker Compose file to include your database connection string:

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

Once ReadySet is up and running, you'll need to [create caches](https://docs.readyset.io/get-started/cache) for the queries you want to speed up.

---

## Documentation

For more information, check out our [documentation](https://docs.readyset.io).

---

## Join the Community

For questions or support, join us on the [ReadySet Community Slack](https://join.slack.com/t/readysetcommunity/shared_invite/zt-1c7bxdxo7-Y6KuoLfc1YWagLk3xHSrsw) to chat with our team.

---

## ReadySet Roadmap
ReadySet is currently in beta. Our team is hard at work stabilizing the system with a focus on PostgreSQL. Our MySQL support is considered alpha. You can learn more about how we're approaching this and follow along on [our roadmap](https://github.com/readysettech/readyset/issues/856).

### Contribute
If you're interested in contributing, we gratefully welcome helping hands! We welcome contributions as [GitHub pull requests](https://github.com/readysettech/readyset/pulls), creating [issues](https://github.com/readysettech/readyset/issues), advocacy, and participating in our [community](#join-the-community)!

### Build from Source
See our [instructions](./community-development.md) on how to build ReadySet from source.

---
## License

ReadySet is licensed under the BSL 1.1 license, converting to the open-source Apache 2.0 license after 4 years. It is free to use on any number of nodes.
