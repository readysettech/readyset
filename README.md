<p align="center">
  <img src="https://user-images.githubusercontent.com/38481289/172237414-023c0b04-c597-44b7-8b14-b5b0c382dc07.png" width='40%'>
</p>

ReadySet is a transparent database cache for Postgres & MySQL that gives you the performance and scalability of an in-memory key-value store without requiring that you rewrite your app or manually handle cache invalidation. ReadySet sits between your application and database and turns even the most complex SQL reads into lightning-fast lookups. Unlike other caching solutions, it keeps cached query results in sync with your database automatically by utilizing your database’s replication stream. It is wire-compatible with Postgres and MySQL and can be used along with your current ORMs and database clients. 

[![Build status](https://badge.buildkite.com/76e02771ab1f0706b7840f47c5fed0e315a56c408d86c0de8c.svg?branch=main)](https://buildkite.com/readyset/readyset-public)
![Number of GitHub contributors](https://img.shields.io/github/contributors/readysettech/readyset)
[![Number of GitHub issues that are open](https://img.shields.io/github/issues/readysettech/readyset)](https://github.com/readysettech/readyset/issues)
[![Number of GitHub stars](https://img.shields.io/github/stars/readysettech/readyset)](https://github.com/readysettech/readyset/stargazers)
![Number of GitHub closed issues](https://img.shields.io/github/issues-closed/readysettech/readyset)
![Number of GitHub pull requests that are open](https://img.shields.io/github/issues-pr-raw/readysettech/readyset)
![GitHub release; latest by date](https://img.shields.io/github/v/release/readysettech/readyset)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/readysettech/readyset)
![Rust](https://img.shields.io/badge/Built%20with%20Rust-grey?logo=rust&logoColor=white)
[![Slack](https://img.shields.io/badge/Join%20Slack-gray?logo=slack&logoColor=white)](https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw)
[![Follow us on X, formerly Twitter](https://img.shields.io/twitter/follow/ReadySet?style=social)](https://twitter.com/readysetio)

:star: If you find ReadySet useful, please consider giving us a star on GitHub! Your support helps us continue to innovate and deliver exciting new features.

## Quickstart
See **[here](https://docs.readyset.io/get-started)**. 

## Useful Links 
* **[Interactive demo](https://docs.readyset.io/demo)**: interactive walk through of ReadySet’s features. 
* **[Getting started guide](https://docs.readyset.io/get-started)**: instructions for how to connect ReadySet to your database and start caching queries. 
* **[Why ReadySet](https://blog.readyset.io/dont-use-kv-stores/)**: explains the motivation behind ReadySet and how it compares to traditional database caching. 
* **[Documentation](https://docs.readyset.io)**: more in-depth information about how to use ReadySet.
* **[Blog](https://blog.readyset.io)**: articles from the ReadySet universe. 


## Community support
For general help using ReadySet, see our **[official docs](https://docs.readyset.io)**. For additional help, you can use one of these channels to ask questions, or give us feedback:
* **[Slack](https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw)**: Discussions with the community and the team.
* **[GitHub](https://github.com/readysettech/readyset/issues/new/choose)**: For bug reports and feature requests.
* **[𝕏 (Twitter)](https://twitter.com/readysetio)**: For product updates and other news. 

## Roadmap
Check out our [roadmap](https://github.com/readysettech/readyset/issues/856) to learn more about what's coming next for ReadySet.

## Contributing
We welcome contributions! Here are a few helpful links to get you started: 
* [Guide to build ReadySet from source](https://github.com/readysettech/readyset/blob/main/community-development.md)
* [Good first issues for first-time contributors](https://github.com/readysettech/readyset/labels/first-issue) 
* [Github issues link to suggest bug fixes and features](https://github.com/readysettech/readyset/issues/new/choose)
* [#source-code channel in Slack to discuss larger projects](https://join.slack.com/t/readysetcommunity/shared_invite/zt-2272gtiz4-0024xeRJUPGWlRETQrGkFw)

## License
ReadySet is licensed under the BSL 1.1 license, converting to the open-source Apache 2.0 license after 4 years. It is free to use on any number of nodes.
