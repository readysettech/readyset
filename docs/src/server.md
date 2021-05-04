# Server

An (incomplete) overview of the components of `noria/server/dataflow/src`. 

## evmap 
`noria/server/dataflow/src/backlog` and `noria/server/dataflow/src/node/special/reader.rs`

Every externally visible view (backed by a data structure called `evmap`) has both a write half and a read half. The write half is part of the data flow graph (referred to as a reader node). The write handle is used to call the swap, replace, refresh functions that take updates to the cached state and makes it visible to read half. The read half of the evmap is not in the data flow graph (it's held by the worker). When a read RPC comes in from a client, it is routed to `noria/server/source/worker/readers.rs` (this is where Noria finds the right read half for that RPC and reads records from it.

## Nodes 

`Node::process` in `noria/server/dataflow/src/node/process.rs` contains all the logic for how an update is executed at a single operator. Most of the operator implementations are in `dataflow/src/ops/`, though some are in `dataflow/src/node/special/`. 

### Sharders 
`noria/server/dataflow/src/node/special/sharder.rs`

Sharder nodes enable the sharding of any domain. Noria create copies of the domain being sharded, but these copies are not in the data flow graph, the system just notes down that the domain is sharded, on which key, and in how many ways. The sharder operator is then placed above any node that's sharded in this way, and ensures that updates are sent to the right downstream domain(s).

## Packets
`Packet` in `dataflow/src/payload.rs`, which holds all the possible messages that a domain may receive. Of particular note are `Packet::Message`, the packet type for inter-domain updates; `Packet::Input`, the packet type for client writes to base tables (see also `noria::Input`); and `Packet::ReplayPiece`, the packet type for upquery responses.

## Domains 
`noria/server/dataflow/src/domain/` 

`Domain` in `noria/server/dataflow/src/domain/mod.rs` contains all the logic used to execute cliques of Noria operators that are contained in the same domain. Its primary entry point is `Domain::on_event`, which gets called whenever there are new `Packets` for the domain. You may also want to look at `Replica` in `src/controller/mod.rs`, which is responsible for managing of all of a domain's inputs and outputs.

`DomainBuilder` struct: 
- Index (UUID for the domain) 
- Shard (Noria shards domains not operators). All operators in a given domain are sharded the same way. Controller gives this info to the domain when it starts it. If shard is None, num shards = 1. Otherwise at least that number, maybe more. 
- Nodes is the list of nodes the domain contains
- Persistence params: flushing timeout, other RocksDB parameters. 
- Config: batching timeout for upquery responses, number of upqueries allowed (set by controller), etc.

The `build` function sets up the initial configuration and state of that domain: 
- `shutdown_valve` (control-C) tells the running domain that it should be shut down
- `readers` map that keeps track of the read halves for every materialized view
- `channel_coordinator` is a bookkeeeping structure that enables domains to talk to eachother by providing an in memory or TCP channel to those domains. 
- `control_addr` is address of the controller

The `on_event` function is called whenever a message arrives for a given domain (e.g. when a controller tells it to update itself, could indicate some change like adding an edge). This function takes an executor (handle back to worker thread that is executing this domain) and matches on the event type. 
- `PollEvent::ResumePolling` is called right before the thread is going to yield/sleep (the domain might then say wake me up in 500ms)
- `PollEvent::Timeout` if registered a timeout, the timeout has now happened. 
- `PollEvent::Process` calls the `handle` function. 
    - The `handle` matches on the type of packet that came in and is the primary entry point for handling evictions, domain updates, data updates, etc. 
        - The `dispatch` function is the data flow execution part of the domain. It handles replays and evictions (called by the `handle` function).
        - The `Packet::PrepareState` message is received when a materialized view is added. 


