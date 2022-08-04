# Noria PostgreSQL adapter

This crate contains the PostgreSQL protocol adapter for
[Noria](https://readyset.io/). The adapter allows legacy applications that use
parameterized PostgreSQL queries to start using Noria with no or minimal source
code changes.

## Running the adapter
To run the adapter and listen on the default PostgreSQL port (5432), type:

```console
$ cargo run --release -- --deployment $NORIA_DEPLOYMENT_ID
```
The `NORIA_DEPLOYMENT_ID` is the same deployment ID you used when starting
the Noria server.

If you would like to use a different port (e.g., because you're also running
a PostgreSQL server), pass `-a <IP>:<PORT>`, making sure to specify the desired
bind ip as well.

## Connecting to Noria
The PostgreSQL adapter uses ZooKeeper to find the Noria server. To specify the
ZooKeeper server location, pass the `--authority-address` argument:

```console
$ cargo run --release -- --deployment $NORIA_DEPLOYMENT_ID --authority-address 172.16.0.19:2181
```
... for a ZooKeeper server listening on port `2181` at IP `172.16.0.19`.
