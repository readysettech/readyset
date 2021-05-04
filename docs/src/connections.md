# Connection Management

Currently, connections to the server go through a connection pool on the client, with one pool per destination ip/port combination. 

The pool size is different for write endpoints and read endpoints ([https://github.com/mit-pdos/noria/blob/6611af503df729d302503e2866dfe896e91ab7e4/noria/src/lib.rs#L153-L181](https://github.com/mit-pdos/noria/blob/6611af503df729d302503e2866dfe896e91ab7e4/noria/src/lib.rs#L153-L181)), because all views hosted by one worker share ip/port, whereas every shard of every base table has a distinct ip/port. 

Each connection is multiplexed (so responses can arrive out-of-order), and has a limit on the number of pending operations on that connection ([https://github.com/mit-pdos/noria/blob/6611af503df729d302503e2866dfe896e91ab7e4/noria/src/lib.rs#L183-L201](https://github.com/mit-pdos/noria/blob/6611af503df729d302503e2866dfe896e91ab7e4/noria/src/lib.rs#L183-L201)). 

Each pool is implemented by having a single spawned future/task that receives requests for the pool over an mpsc channel, which then routes the request to a particular connection. 

The connection serializes the request out on the wire, deserializes responses, and feeds them back to the appropriate request originator over single-use channels. 

The full type "sandwich" is here: [https://github.com/mit-pdos/noria/blob/11a10b7e2257ee687a60de9a5c692f51d1d75f2b/noria/src/view.rs#L90](https://github.com/mit-pdos/noria/blob/11a10b7e2257ee687a60de9a5c692f51d1d75f2b/noria/src/view.rs#L90). 

The inner connection type is a [https://docs.rs/tokio-tower/0.5.0/tokio_tower/multiplex/client/struct.Client.html](https://docs.rs/tokio-tower/0.5.0/tokio_tower/multiplex/client/struct.Client.html), and the thing that routes among connections is a [https://docs.rs/tower-balance/0.3.0/tower_balance/p2c/struct.Balance.html](https://docs.rs/tower-balance/0.3.0/tower_balance/p2c/struct.Balance.html). 

The mpsc channel part to get many handles to a pool is [https://docs.rs/tower-buffer/0.3.0/tower_buffer/struct.Buffer.html](https://docs.rs/tower-buffer/0.3.0/tower_buffer/struct.Buffer.html).

