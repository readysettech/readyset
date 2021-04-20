# psql-srv

Bindings for emulating a PostgreSQL server.

When developing new databases or caching layers, it can be immensely useful to test your system
using existing applications. However, this often requires significant work modifying
applications to use your database over the existing ones. This crate solves that problem by
acting as a PostgreSQL server, and delegating operations such as querying and query execution to
user-defined logic.

To start, implement `Backend` for your backend, and call `run_backend` on an instance of your
backend along with a connection stream. The appropriate methods will be called on
your backend whenever a client sends a `QUERY`, `PARSE`, `DESCRIBE`, `BIND` or `EXECUTE` message,
and you will have a chance to respond appropriately. See `lib.rs` for information on implementing
a `Backend` and `ServeOneBackend`, in the `serve_one.rs` example, for a very basic implementation.
