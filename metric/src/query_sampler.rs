/// Gauge: The number of queries in the query sampler queue.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | queue_len | The number of queries in the queue |
pub const QUERY_SAMPLER_QUEUE_LEN: &str = "readyset_query_sampler_queue_len";

/// Counter: The number of queries sampled by the query sampler.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | queries_sampled | The number of queries sampled |
pub const QUERY_SAMPLER_QUERIES_SAMPLED: &str = "readyset_query_sampler_queries_sampled";

/// Counter: The number of queries mismatched by the query sampler.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | queries_mismatched | The number of queries mismatched |
pub const QUERY_SAMPLER_QUERIES_MISMATCHED: &str = "readyset_query_sampler_queries_mismatched";

/// Gauge: The number of entries in the retry queue.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | retry_queue_len | The number of entries in the retry queue |
pub const QUERY_SAMPLER_RETRY_QUEUE_LEN: &str = "readyset_query_sampler_retry_queue_len";

/// Counter: The number of times the retry queue is full.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | retry_queue_full | The number of times the retry queue is full |
pub const QUERY_SAMPLER_RETRY_QUEUE_FULL: &str = "readyset_query_sampler_retry_queue_full";

/// Counter: The number of times the sampler is reconnecting to the upstream database.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | reconnects | The number of times the sampler is reconnecting to the upstream database |
pub const QUERY_SAMPLER_RECONNECTS: &str = "readyset_query_sampler_reconnects";

/// Counter: The number of times the sampler has hit the max QPS.
///
/// | Tag | Description |
/// | --- | ----------- |
/// | max_qps | The number of times the sampler has hit the max QPS |
pub const QUERY_SAMPLER_MAX_QPS_HIT: &str = "readyset_query_sampler_max_qps_hit";
