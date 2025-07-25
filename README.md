<p align="center">
  <img src="https://readyset.io/images/Brand_Logo_Dark.svg" width='80%'>
</p>

Readyset is a transparent database cache for Postgres & MySQL that gives you the performance and scalability of an in-memory key-value store without requiring that you rewrite your app or manually handle cache invalidation. Readyset sits between your application and database and turns even the most complex SQL reads into lightning-fast lookups. Unlike other caching solutions, it keeps cached query results in sync with your database automatically by utilizing your database’s replication stream. It is wire-compatible with Postgres and MySQL and can be used along with your current ORM or database client.

![Build status](https://badge.buildkite.com/dff06f78d8fee6a0a4f892c6831ff42ce566cee6405a14f395.svg)
![Number of GitHub contributors](https://img.shields.io/github/contributors/readysettech/readyset)
[![Number of GitHub issues that are open](https://img.shields.io/github/issues/readysettech/readyset)](https://github.com/readysettech/readyset/issues)
[![Number of GitHub Repo stars](https://img.shields.io/github/stars/readysettech/readyset?style=flat)](https://github.com/readysettech/readyset/stargazers)
![Number of GitHub closed issues](https://img.shields.io/github/issues-closed/readysettech/readyset)
![Number of GitHub pull requests that are open](https://img.shields.io/github/issues-pr-raw/readysettech/readyset)
![GitHub release; latest by date](https://img.shields.io/github/v/release/readysettech/readyset)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/readysettech/readyset)
![Built with Rust](https://img.shields.io/badge/Built%20with%20Rust-grey?logo=rust)
[![Slack](https://img.shields.io/badge/Join%20Slack-gray?logo=slack&logoColor=white)](https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw)
[![Follow us on X, formerly Twitter](https://img.shields.io/twitter/follow/readysetio?style=flat&logo=x)](https://twitter.com/readysetio)

:star: If you find Readyset useful, please consider giving us a star on GitHub! Your support helps us continue to innovate and deliver exciting new features.

## Quickstart

To get started in five minutes or less, run:

```
bash -c "$(curl -sSL https://launch.readyset.io)"
```

You can also install via a [Docker image](https://docs.readyset.io/get-started/install-rs/docker) or with a [Linux binary](https://docs.readyset.io/get-started/install-rs/binaries). See our [getting started guide](https://docs.readyset.io/get-started) for more details!

Readyset Cloud is a managed service that scales your database with ease. If you're interested in trying out Readyset Cloud, [try it today](https://readyset.cloud/)!


## Useful Links
* **[Interactive demo](https://docs.readyset.io/demo)**: interactive walk through of Readyset’s features.
* **[Getting started guide](https://docs.readyset.io/get-started)**: instructions for how to connect Readyset to your database and start caching queries.
* **[Why Readyset](https://blog.readyset.io/dont-use-kv-stores/)**: explains the motivation behind Readyset and how it compares to traditional database caching.
* **[Documentation](https://docs.readyset.io)**: more in-depth information about how to use Readyset.
* **[Blog](https://blog.readyset.io)**: articles from the Readyset universe.


## Community support
For general help using Readyset, see our **[official docs](https://docs.readyset.io)**. For additional help, you can use one of these channels to ask questions, or give us feedback:
* **[Slack](https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw)**: Discussions with the community and the team.
* **[GitHub](https://github.com/readysettech/readyset/issues/new/choose)**: For bug reports and feature requests.
* **[𝕏 (Twitter)](https://twitter.com/readysetio)**: For product updates and other news.

## Contributing
We welcome contributions! Here are a few helpful links to get you started:
* [Guide to build Readyset from source](https://github.com/readysettech/readyset/blob/main/community-development.md)
* [Good first issues for first-time contributors](https://github.com/readysettech/readyset/labels/first-issue)
* [Github issues link to suggest bug fixes and features](https://github.com/readysettech/readyset/issues/new/choose)
* [#source-code channel in Slack to discuss larger projects](https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw)

## License
Readyset is licensed under the BSL 1.1 license, converting to the open-source Apache 2.0 license after 4 years. It is free to use on any number of nodes.
