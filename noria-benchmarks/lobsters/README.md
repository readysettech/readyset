# Noria Lobsters benchmark

This benchmark uses [`trawler`](https://github.com/jonhoo/trawler) to
generate a workload that is similar to that of the
[lobste.rs](https://lobste.rs) news aggregator. It is the same benchmark
that was used in §8.1 in the [OSDI'18 Noria
paper](https://jon.tsp.io/papers/osdi18-noria.pdf).

The benchmark issues SQL queries from the [Lobsters code
base](https://github.com/lobsters/lobsters) according to the production
traffic patterns reported [here](https://lobste.rs/s/cqnzl5/). It can
run in one of three modes:

 - `mysql`: the exact SQL queries issued by Lobsters.
 - `noria`: the original Lobsters queries with minor tweaks to make them
   work with Noria.
 - `natural`: queries without manual pre-computations.

The Noria benchmarks should all be run through the [MySQL
adapter](https://github.com/mit-pdos/noria-mysql).

Compile the benchmark with

```console
$ cargo b --release --bin lobsters
```

Then start the backend you want to benchmark, you must first decide what
target load (`reqscale`) you want to test. The `reqscale` is a scaling
factor relative to that seen by the production Lobsters deployment. See
the paper linked above for good values to try.

Then, run, for example, with Noria's natural queries:

```console
$ target/release/lobsters \
   --warmup 0 \
   --runtime 0 \
   --issuers 24 \
   --queries natural \
   --prime \
   "mysql://lobsters:soup@127.0.0.1/lobsters"
$ target/release/lobsters \
   --reqscale 3000 \
   --warmup 120 \
   --runtime 0 \
   --issuers 24 \
   --queries natural \
   "mysql://lobsters:soup@127.0.0.1/lobsters"
$ target/release/lobsters \
   --reqscale <target> \
   --warmup 20 \
   --runtime 30 \
   --issuers 24 \
   --queries natural \
   "mysql://lobsters:soup@127.0.0.1/lobsters"
```

The first command primes the backend with initial users, stories, and
comments. The second warms up the database. The last issues requests at
the given `target` rate for 50 seconds (discarding the first 20s) and
reports the observed latencies for the different Lobsters pages.

To run a different `reqscale`, you only need to run the last command.

## Running the Noria backend

Make sure ZooKeeper is running on the server machine. Then use the
following command to run `noria-server`:

```console
$ cargo r --release --bin noria-server -- \
   --deployment x --durability memory --no-reuse \
   --address <srv-ip> --shards 0 -v
```

And this to run the MySQL adapter on the clients:

```console
$ cargo r --release -- \
   --deployment x --no-sanitize --no-static-responses \
   -z <srv-ip>:2181 -p 3306 -v
```

Where `srv-ip` is the server's external IP address.
