# Controller

## Startup Process and Controller Overview

`noria/server/src/main.rs` starts a Noria server process (after parsing command line options) and then calls `noria/server/src/startup.rs`, which sets up the worker half and the controller half of the instance. It does this for every single server instance, regardless of whether there is a controller already running. 

`noria/server/src/controller/mod.rs` function `main` is called by `startup.rs` to start up the controller half. The controller half is given handles to many things in process including `Authority`. `Authority` decides which controller half is the actual controller. In a distributed Noria deployment, the `Authority` is Zookeeper. In single node 
deployments, it's an in memory channel. 

`noria/server/src/controller/mod.rs` function `main` calls `listen_domain_replies`, which is the channel that the controller uses to receive messages from workers ("heartbeats"). 

`instance_campaign` starts out in the worker state, waits for a different node to become the controller (leader), calls the `Authority` to figure out the current leader, and then loops waiting for a different node to become the leader. If it fails, it goes into election state itself (i.e. starts a leader election campaign). If it succeeds, then this node adopts the state of the previous controller and steps in to manages all of the workers (i.e. the rest of controller logic kicks in). If this node won leader election, then it will also send out an event message saying that it won and that the leader has changed to its local worker. The local worker, in turn, "knows" that it's the leader (and that no one else is the leader). 

The `main` function does message handling (receiving heartbeats, RPCs, migration requests, etc). 

`noria/server/src/controller/inner.rs` implements most of the controller logic, this is only created if an instance's controller half wins leader election. 
`ControllerInner` is held by a *single* worker, and "owns" the currently executing data-flow graph. If a client wishes to add or modify queries, create new handles to base tables or views, or inspect the current data-flow, they end up interfacing with a `ControllerInner`.

The `ControllerInner` struct contains a lot of bookkeeping information: 
- `ingredients`: list of all operators and the current dataflow graph (base tables, operators, edges, full graph info) 
- `materializations`: state of which nodes are materialized and in which way (i.e. full, partial, etc)
- `recipe`: MIR-level representation of the current dataflow graph (SQL statements the controller knows about, instantiated in the dataflow stored in ingredients). Migration works by app providing new queries which are then combined with the old queries (ingredients change)
- `domains`, `domain_handle`:  handles to each domain that lets you send messages to it (e.g. add this operator, add this edge)
- `domain_nodes`: reverse index, given a domain id you can find out what indices (nodes) are in it
- `channel_coordinator`: way to get a channel to any arbitrary domain- if domain X says it needs to talk to domain Y, tells that to channel coordinator which will give that to domain X
- `read_addrs`: every worker runs a single read server across all of their domains (what is the address for which we should send reads for every worker i know about) 
- `workers`: for each worker ID, keep track of state about that worker (last heartbeat, address, current state (up/down), etc
- `remap`: keeps track of replacements of parent nodes when adding ingress/egress/sharders etc into the actual data flow graph of SQL operators
- `epoch`: keeps track of controller epoch so that controllers can realize it's no longer the current controller in the face of network parititions etc
- `pending_recovery`: keeps track of whether currently recovering anything for a node etc if it went down
- `domain_replies`: abstracts away the controller logic from how it receives messages. sets up TCP server, receives messages from servers, and creates in memory channel. 
- `controller_inner`: receives messages on this channel.

