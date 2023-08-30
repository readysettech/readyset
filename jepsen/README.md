# Jepsen tests for ReadySet

This directory contains a suite of [jepsen][] tests for ReadySet. We run them
periodically to test for ReadySet's eventual consistency and liveness guarantees
under a high-availability configuration.

[jepsen]: https://github.com/jepsen-io/jepsen

## Running Jepsen tests

First, you'll need a cluster of nodes to run the tests on. Jepsen works by
SSHing into a pool of nodes to set up the database, then issuing commands to the
nodes over the network. To test a full high-availability configuration, you need
a minimum of 6 nodes: one for each of the load balancer, upstream DB, consul,
and `readyset-server`, and two for the ReadySet adapters.

During development of these Jepsen tests, I set up a cluster of 6 EC2 instances
using the AWS console then installed Tailscale on them. This was the simplest
way to allow all the nodes to network to each other on all ports, but if you're
following along at home and don't have Tailscale easily accessible, you'll
probably just want to open up all ports between each node, and at the very least
ports 22, 5432, and 8500 from the machine you're running Jepsen on to each node.

As far as the instances go, this test is written to use Ubuntu Server (but would
probably work, perhaps with minimal modification, on Debian). The
readyset-server and readyset-adapter instances will compile the ReadySet
binaries in release mode, so you'll want to make them big enough. During
development I used `c4.2xlarge` instances. You'll also need to make sure you
have enough disk space - generally 100GB should be enough for the root volume.

Once you have your instances set up and accessible over SSH, you can run the
Jepsen test. The easiest way to configure jepsen to use your instances is the
`nodes` file - just put the hostname or IP of the instances, one per line, in a
file called `nodes` in the jepsen directory. Then you can run the Jepsen test
like this after installing [leiningen][]:

[leiningen]: https://leiningen.org/

```shellsession
❯ lein run test \
    --nodes-file ./nodes \
    --username ubuntu \ # or whatever your SSH username is
    --ssh-private-key ~/.ssh/my-key.pem \ # or whatever your private key is
    --time-limit 60 \ # how long to run the test, in seconds
    --concurrency 10 \ # how many concurrent clients to run
    --rate 50 # operations generated per second
```

For additional options, there's decent command-line documentation available -
just run:

```shellsession
❯ lein run test --help
```

After you've run a few tests, you might want to inspect the results. Jepsen
comes with a nice web server which can be used to browse test results, including
some cool HTML visualizations of the history of the test. Just run:

``` shellsession
❯ lein run serve
```

and open up http://localhost:8080 in your browser.
