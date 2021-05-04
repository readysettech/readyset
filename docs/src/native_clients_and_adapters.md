# Native Clients and Adapters

All native client libraries, such as the [Node.js client library](js_client.md), are expected to provide a thin wrapper around the [`noria-client`](./noria_client.md). They are designed to provide an experience that resembles using popular MySQL clients for that language.

Adapters, such as the MySQL adapter and the PostgreSQL adapter, each run servers that appear to be an actual MySQL/PostgreSQL server. This allows third-party MySQL/PostgreSQL clients to connect to this server, which then communicates with a [`noria-client`](./noria_client.md) to carry out queries.
