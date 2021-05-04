# Read Path 

**Read path beginning from view.rs** [[noria/src/view.rs](https://github.com/readysettech/readyset/blob/e37e304ed5654d1690fdd131175b062b7487edbb/noria/noria/src/view.rs)]

`View.call`

1. Assign view queries to their respective shards as ReadQuery requests by sending messages to reader workers [[server/src/worker/reader.rs](https://github.com/readysettech/readyset/blob/e37e304ed5654d1690fdd131175b062b7487edbb/noria/server/src/worker/readers.rs#L362)].
2. The read workers will asynchronously return a  `ReadReply<SerializedReadReplyBatch>`. This will involve blocking when the keys required are not available.
    1. The correct reader worker is retrieved by using `(NodeIndex, usize)` as a key into a map of readers. This will return a `SingleReadHandle`, the state of a single shard of a reder.
3. When handling a normal read query attempt to look up using `do_lookup`, we track all the keys that are a miss at the reader. We trigger a reply for missing keys that are part of the materialized view by calling `trigger` [dataflow/src/backlog/mod.rs].