# Controller Handle

Like most databases, Noria follows a server-client model where many clients connect to a
(potentially distributed) server. The server in this case is the `noria-server`
binary, and must be started before clients can connect. See `noria-server --help` for details
and the [Noria repository README](https://github.com/mit-pdos/noria) for details. Noria uses
[Apache ZooKeeper](https://zookeeper.apache.org/) to announce the location of its servers, so
ZooKeeper must also be running.

A ControllerHandle is, unsurprisingly, a handle to a Noria controller.
This handle allows interaction with a running Noria instance and lets
you add and remove queries, retrieve handles for inserting or querying the underlying data, and perform meta-operations such as fetching the dataflow's GraphViz visualization.
________________
** Note: we are working on a new version of the docs below. ETA: end of the week of May 3rd **
`noria/noria`

---

`consensus` dictates how noria talks to the thing that tells it where the controller is (consensus/mod authority trait, defines what the consensus mechanisms that decide controller look like/ how to interact with it) 

`get_leader` gives you serialized JSON that describes the current controller. 

Only two authorities currently implemented. Local: in memory mutex. Zookeeper as the other option (create entries in zookeeper, gets data on static key, and if it gets result back it deserializes the bytes it gets). Here we could implement another authority (e.g. local file, chubby, etcd, whatever else). 

`channel` abstraction over in-memory vs TCP channels. 

`channel coordinator`: have address of other worker, get TCP / in memory channel to contact it. 

LocalBypass: when client and server in same process, skip serialization, do pointers instead (very unsafe, grep for i_promise for method that enables this) 

`[data.rs](http://data.rs)` datatype definitions + various helper methods. 

`table.rs`

`view.rs`

handle types for base tables and views. 

`ViewBuilder` gives you everything you need to know to access view in the client. Same idea for `TableBuilder`. Both have a `build` method, which client library calls to get TCP connection to the object. Internally connects to all addresses listed. 

Per-client load balancing between connections.

`extend_recipe` install new queries and schemas.

`src/lib.rs` has a lot of constants set for benchmarking, would prob be changed for production, and would probably not be constants (dynamically set instead). 

`BUFFER_TO_POOL` length of in memory channel for fan in 

`TABLE_POOL_SIZE` how many connections does noria establish to given basetable to load balance across 

`VIEW_POOL_SIZE` number of connections to readers 

The reason we want these numbers to be larger than 1 is because then you could do serialization across multiple threads. 

`PENDING_LIMIT` how many requests can be in flight at a given point in time

`View` type primarily goes through implementation of `Service`. Args: key/cols & whether or not should block. 

`Table Input` method: takes input (vec of table operations) and sends it as a request 

`inject_dropped_column` fixes up new new writes to include old cols that used to be there in the past (important for migrations without downtime)

`macros: row & update` (in `table.rs`) ergonomic way to construct inserts / updates by using column names rather than numbers
