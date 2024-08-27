//! Documents the set of metrics that are currently being recorded within
//! instances of a reader-map.

/// Histogram: The time interval in milliseconds between updates to a given key in the reader-map.
/// Updates include inserts, updates, and deletions from the key's results set.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | node_index | The node index of the reader node in the dataflow graph |
pub const READER_MAP_UPDATES: &str = "readyset_reader.entry_updates_ms";

/// Histogram: The time in milliseconds that an entry was "live" in the `reader-map`, as measured
/// from first insert to eviction.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | node_index | The node index of the reader node in the dataflow graph |
pub const READER_MAP_LIFETIMES: &str = "readyset_reader.entry_lifetimes_ms";
