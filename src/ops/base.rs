use serde_json;
use snowflake::ProcessUniqueId;
use buf_redux::BufWriter;
use buf_redux::strategy::WhenFull;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use time;

// 4k buffered log.
const LOG_BUFFER_CAPACITY: usize = 4 * 1024;

// We store at most this many write records before flushing to disk.
const BUFFERED_WRITES_CAPACITY: usize = 512;

// We spend at least this many milliseconds without flushing write records to disk.
const BUFFERED_WRITES_FLUSH_INTERVAL_MS: u64 = 1000;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug)]
pub struct Base {
    buffered_writes: Option<Records>,
    durability: Option<BaseDurabilityLevel>,
    durable_log: Option<BufWriter<File, WhenFull>>,
    durable_log_path: Option<PathBuf>,
    last_flushed_at: Option<Instant>,
    primary_key: Option<Vec<usize>>,
    should_delete_log_on_drop: bool,

    // This id is unique within the same process.
    //
    // TODO(jmftrindade): Figure out the story here.  While ProcessUniqueId is guaranteed to be
    // unique within the same process, the assignment of ids is not deterministic across multiple
    // process runs. This is just a tuple of 2 monotonically increasing counters: the first is per
    // process, and the second is "within" that process.
    //
    // We should instead make sure that Base nodes remember their own global_address at creation
    // (or perhaps a globally unique id assigned by a recovery manager), and use that as identifier
    // for durable log filename.
    unique_id: ProcessUniqueId,

    us: Option<NodeAddress>,
}

/// Specifies the level of durability that this base node should offer. Stronger guarantees imply a
/// reduced write performance.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BaseDurabilityLevel {
    /// Buffered writes: records are accumulated in an in-memory buffer and occasionally flushed to
    /// the durable log, which may itself buffer in the file system. Results in large batched
    /// writes, but offers no durability guarantees on crashes.
    Buffered,
    /// Synchronous writes: forces every record to be written to disk before it is emitted further
    /// into the data-flow graph. Strong guarantees (writes are never lost), but high performance
    /// penalty.
    SyncImmediately,
}

impl Base {
    /// Create a non-durable base node operator.
    pub fn new(primary_key: Vec<usize>) -> Self {
        let mut base = Base::default();
        base.primary_key = Some(primary_key);
        base
    }

    /// Create durable base node operator.
    pub fn new_durable(primary_key: Vec<usize>, durability: BaseDurabilityLevel) -> Self {
        let mut base = Base::new(primary_key);
        base.durability = Some(durability);
        base
    }

    /// Whether this base node should delete its durable log on drop.  Used when durable log is not
    /// intended to be used for future recovery, e.g., on tests.
    pub fn delete_log_on_drop(mut self) -> Base {
        self.should_delete_log_on_drop = true;
        self
    }

    /// Write records to durable log.
    fn persist_to_log(&mut self, records: &Records) {
        match self.durability {
            None => panic!("tried to persist non-durable base node!"),
            Some(BaseDurabilityLevel::Buffered) |
            Some(BaseDurabilityLevel::SyncImmediately) => {
                self.ensure_log_writer();
                serde_json::to_writer(&mut self.durable_log.as_mut().unwrap(), &records)
                    .unwrap();
                // XXX(malte): we must deconstruct the BufWriter in order to get at the contained
                // File (on which we can invoke sync_data(), only to then reassemble it
                // immediately. I suspect this will work best if we flush after accumulating
                // batches of writes.
                let file = self.durable_log.take().unwrap().into_inner().unwrap();
                // need to drop as sync_data returns Result<()> and forces use
                drop(file.sync_data());
                self.durable_log = Some(BufWriter::with_capacity_and_strategy(
                    LOG_BUFFER_CAPACITY, file, WhenFull));
            }
        }
    }

    /// Open durable log and initialize a buffered writer to it if successful.
    fn ensure_log_writer(&mut self) {
        match self.durability {
            None => {
                panic!("tried to create a log for non-durable base node!");
            },

            Some(BaseDurabilityLevel::Buffered) |

            // XXX(jmftrindade): buf_redux does not provide a "sync immediately" flush strategy
            // out of the box, so we handle that from persist_to_log by dropping sync_data.
            Some(BaseDurabilityLevel::SyncImmediately) => {

                let us = self.us.expect("on_input should never be called before on_commit");

                if self.durable_log.is_none() {
                    let now = time::now();
                    let today = time::strftime("%F", &now).unwrap();

                    // TODO(jmftrindade): Make a base node remember its own global address so that
                    // we can use that as unique_id for durable logs instead of process unique ids.
                    //
                    // let log_filename = format!("/tmp/soup-log-{}-{:?}-{}.json",
                    //                           today, self.global_address.unwrap(), self.unique_id);

                    let log_filename = format!("/tmp/soup-log-{}-{}-{}.json",
                                               today, us, self.unique_id);
                    self.durable_log_path = Some(PathBuf::from(&log_filename));

                    if let Some(ref path) = self.durable_log_path {
                        // TODO(jmftrindade): Current semantics is to overwrite an existing log.
                        // Once we have recovery code, we obviously do not want to overwrite this
                        // log before recovering.
                        let file = match OpenOptions::new()
                            .read(false)
                            .append(false)
                            .write(true)
                            .create(true)
                            .open(path) {
                            Err(reason) => {
                                panic!("Unable to open durable log file {}, reason: {}",
                                       path.display(), reason)
                            }
                            Ok(file) => file,
                        };

                        self.durable_log = Some(BufWriter::with_capacity_and_strategy(
                            LOG_BUFFER_CAPACITY, file, WhenFull))
                    }
                }
            }
        }
    }

    /// XXX: This should only be used by tests.  We don't hide it behind cfg test, however, since it
    /// needs to be available for integration tests, which get compiled against the regular build.
    pub fn delete_durable_log(&mut self) {
        // Cleanup any durable log files.
        if let Some(ref path) = self.durable_log_path {
            fs::remove_file(path).unwrap();
        }
    }

    /// Flush any buffered writes, and clear the buffer, returning all flushed writes.
    pub fn flush(&mut self) -> Records {
        let flushed_writes = self.buffered_writes.as_mut().unwrap().drain(..).collect();
        self.persist_to_log(&flushed_writes);
        self.last_flushed_at = Some(Instant::now());

        return flushed_writes
    }
}

/// A Base clone must have a different unique_id so that no two copies write to the same file.
/// Resetting the writer to None in the original copy is not enough to guarantee that, as the
/// original object can still re-open the log file on-demand from Base::persist_to_log.
impl Clone for Base {
    fn clone(&self) -> Base {
        Base {
            buffered_writes: self.buffered_writes.clone(),
            durability: self.durability,
            durable_log: None,
            durable_log_path: None,
            last_flushed_at: self.last_flushed_at,
            primary_key: self.primary_key.clone(),
            should_delete_log_on_drop: self.should_delete_log_on_drop,
            unique_id: ProcessUniqueId::new(),
            us: self.us,
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            buffered_writes: Some(Records::default()),
            durability: None,
            durable_log: None,
            durable_log_path: None,
            last_flushed_at: Some(Instant::now()),
            primary_key: None,
            should_delete_log_on_drop: false,
            unique_id: ProcessUniqueId::new(),
            us: None,
        }
    }
}

impl Drop for Base {
    fn drop(&mut self) {
        if self.should_delete_log_on_drop {
            self.delete_durable_log();
        }
    }
}

use flow::prelude::*;

impl Ingredient for Base {
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, materialized: bool) -> bool {
        !materialized && self.primary_key.is_some()
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeAddress, _: &HashMap<NodeAddress, NodeAddress>) {
        self.us = Some(us);
    }

    fn on_input(&mut self,
                _: NodeAddress,
                mut rs: Records,
                _: &DomainNodes,
                state: &StateMap)
                -> Records {
        // Write incoming records to log before processing them if we are a durable node.
        let records_to_return;
        match self.durability {
            Some(BaseDurabilityLevel::Buffered) => {
                // Perform a synchronous flush if one of the following conditions are met:
                //
                // 1. Enough time has passed since the last time we flushed.
                // 2. Our buffer of write records reaches capacity.
                let num_buffered_writes = self.buffered_writes.as_ref().unwrap().len();
                let has_reached_capacity = num_buffered_writes + rs.len() >= BUFFERED_WRITES_CAPACITY;
                let elapsed = self.last_flushed_at.unwrap().elapsed();
                let has_reached_time_limit = elapsed >= Duration::from_millis(BUFFERED_WRITES_FLUSH_INTERVAL_MS);

                if has_reached_capacity || has_reached_time_limit {
                    self.buffered_writes.as_mut().unwrap().append(&mut rs);

                    // This returns everything that was buffered, plus the newly inserted records.
                    records_to_return = self.flush();
                } else {
                    // Otherwise, buffer the records and don't send them downstream.
                    self.buffered_writes.as_mut().unwrap().append(&mut rs);
                    return Records::default()
                }
            },
            Some(BaseDurabilityLevel::SyncImmediately) => {
                self.persist_to_log(&rs);
                records_to_return = rs;
            },
            None => {
                records_to_return = rs;
            },
        }

        records_to_return.into_iter()
            .map(|r| match r {
                     Record::Positive(u) => Record::Positive(u),
                     Record::Negative(u) => Record::Negative(u),
                     Record::DeleteRequest(key) => {
                let cols = self.primary_key
                        .as_ref()
                        .expect("base must have a primary key to support deletions");
                let db =
                    state.get(self.us
                                  .as_ref()
                                  .unwrap()
                                  .as_local())
                        .expect("base must have its own state materialized to support deletions");
                let rows = db.lookup(cols.as_slice(), &KeyType::from(&key[..]));
                assert_eq!(rows.len(), 1);

                Record::Negative(rows[0].clone())
            }
                 })
            .collect()
    }

    fn suggest_indexes(&self, n: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        if self.primary_key.is_some() {
            Some((n,
                  self.primary_key
                      .as_ref()
                      .unwrap()
                      .clone()))
                    .into_iter()
                    .collect()
        } else {
            HashMap::new()
        }
    }

    fn resolve(&self, _: usize) -> Option<Vec<(NodeAddress, usize)>> {
        None
    }

    fn is_base(&self) -> bool {
        true
    }

    fn description(&self) -> String {
        "B".into()
    }

    fn parent_columns(&self, _: usize) -> Vec<(NodeAddress, Option<usize>)> {
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_default() {
        let b = Base::default();
        assert!(b.buffered_writes.is_some());
        assert_eq!(b.buffered_writes.as_ref().unwrap().len(), 0);
        assert!(b.durability.is_none());
        assert!(b.durable_log.is_none());
        assert!(b.durable_log_path.is_none());
        assert!(b.primary_key.is_none());
        assert_eq!(b.should_delete_log_on_drop, false);
        assert!(b.us.is_none());
    }

    #[test]
    fn it_works_new() {
        let b = Base::new(vec![0]);
        assert!(b.buffered_writes.is_some());
        assert_eq!(b.buffered_writes.as_ref().unwrap().len(), 0);
        assert!(b.durability.is_none());
        assert!(b.durable_log.is_none());
        assert!(b.durable_log_path.is_none());
        assert_eq!(b.primary_key.as_ref().unwrap().len(), 1);
        assert_eq!(b.should_delete_log_on_drop, false);
        assert!(b.us.is_none());
    }

    #[test]
    fn it_works_durability_buffered() {
        let b = Base::new_durable(vec![0], BaseDurabilityLevel::Buffered);
        assert!(b.buffered_writes.is_some());
        assert_eq!(b.buffered_writes.as_ref().unwrap().len(), 0);
        assert_eq!(b.durability, Some(BaseDurabilityLevel::Buffered));
        assert!(b.durable_log.is_none());
        assert!(b.durable_log_path.is_none());
        assert_eq!(b.primary_key.as_ref().unwrap().len(), 1);
        assert_eq!(b.should_delete_log_on_drop, false);
        assert!(b.us.is_none());
    }

    #[test]
    fn it_works_durability_sync_immediately() {
        let b = Base::new_durable(vec![0], BaseDurabilityLevel::SyncImmediately);
        assert!(b.buffered_writes.is_some());
        assert_eq!(b.buffered_writes.as_ref().unwrap().len(), 0);
        assert_eq!(b.durability, Some(BaseDurabilityLevel::SyncImmediately));
        assert!(b.durable_log.is_none());
        assert!(b.durable_log_path.is_none());
        assert_eq!(b.primary_key.as_ref().unwrap().len(), 1);
        assert_eq!(b.should_delete_log_on_drop, false);
        assert!(b.us.is_none());
    }
}
