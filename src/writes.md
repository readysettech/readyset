# Write Path

**Write path beginning from table.rs**
The set of operations done on base tables is defined in `TableOperation` [[noria/src/data.rs](https://github.com/readysettech/readyset/blob/229a110ae1e11db1f3fd1acb053f2cc1f9143e36/noria/noria/src/data.rs#L1032)].

`Table.insert`, `Table.update`, and `Table.delete` each create a corresponding `TableOperation` [[noria/src/data.rs](https://github.com/readysettech/readyset/blob/229a110ae1e11db1f3fd1acb053f2cc1f9143e36/noria/noria/src/data.rs#L1032)]., the set of operations that may be performed on base tables.

These operations are placed in an `Input` wrapping a domain-local node identifier with it's `TableOperation` and passed to `Table.input`.

`Table.input`
1. Assign table operations to their respective base table shards. If there is one shard all operations are going to the shard.
2. For each <shard, operation>  pair: create a request that may refer to a domain-local or remote shard. 
    - A request is just the `Input` wrapped by `LocalOrNot`, this allows the rpcs to go to potentially remote domains.
3. Issue RPCs to shards corresponding to the request. The shards are each wrapped by a tower_buffer::Buffer to buffer rpc calls. `self.shards[s]` returns the buffer and `call` [[tower_buffer::Buffer::call](https://docs.rs/tower-buffer/0.3.0/tower_buffer/struct.Buffer.html#method.call)] passes the request to the buffer and waits for a response.
    1. The connection here is through replica.rs
4. A packet will be sent to the relevant domain of the base node. Where depending on the packet type, it will call handle on the packet or append the packet to a group commit queue.
    - For handling, see: Domain::handle [[dataflow/src/domain/mod.rs](https://github.com/readysettech/readyset/blob/229a110/noria/server/dataflow/src/domain/mod.rs#L3069)]
    - For a base table node this will find it's way to Node::process [[dataflow/src/node/process.rs](https://github.com/readysettech/readyset/blob/229a110ae1e11db1f3fd1acb053f2cc1f9143e36/noria/server/dataflow/src/node/process.rs#L62)] on the NodeType::Base. The input will be handled Base::process [[dataflow/src/node/special/base.rs](https://github.com/readysettech/readyset/blob/229a110ae1e11db1f3fd1acb053f2cc1f9143e36/noria/server/dataflow/src/node/special/base.rs#L135)].
    - The base table node will convert the Packet::Input into a Packet::Message and propagate this to downstream nodes.
    - Materialization may occur when internal nodes are handling the Packet::Message that has the changes to the base table.
5. This creates a vector of positive and negative records (additions and removals) from the set of records. This set of mutations is used to modify the `Packet`.