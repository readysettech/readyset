// TODO(jmftrindade): Put the writing to disk behind a runtime command-line flag.
use serde_json;
use snowflake::ProcessUniqueId;
use buf_redux::BufWriter;
use buf_redux::strategy::WhenFull;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use time;

// 4k buffered log.
const LOG_BUFFER_CAPACITY: usize = 4 * 1024;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug)]
pub struct Base {
    durability: BaseDurabilityLevel,
    durable_log: Option<BufWriter<File, WhenFull>>,
    durable_log_path: Option<PathBuf>,
    global_address: Option<NodeAddress>,
    key_column: Option<usize>,

    // This id is unique within the same process.
    //
    // TODO(jmftrindade): Figure out the story here.  While ProcessUniqueId is guaranteed to be
    // unique within the same process, the assignment of ids is not deterministic across multiple
    // process runs. This is just a tuple of 2 monotonically increasing counters: the first is per
    // process, and the second is "within" that process.
    unique_id: ProcessUniqueId,

    us: Option<NodeAddress>,
}

/// Specifies the level of durability that this base node should offer. Stronger guarantees imply a
/// reduced write performance.
#[derive(Clone, Copy, Debug)]
pub enum BaseDurabilityLevel {
    /// No durability at all: records aren't written to a log.
    None,
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
    /// Create a base node operator.
    pub fn new(key_column: usize, durability: BaseDurabilityLevel) -> Self {
        Base {
            key_column: Some(key_column),
            durability: durability,
            durable_log: None,
            durable_log_path: None,
            global_address: None,
            us: None,
            unique_id: ProcessUniqueId::new(),
        }
    }

    /// Write records to durable log.
    fn persist_to_log(&mut self, records: &Records) {
        match self.durability {
            BaseDurabilityLevel::None => panic!("tried to persist non-durable base node!"),
            BaseDurabilityLevel::Buffered => {
                self.ensure_log_writer();
                serde_json::to_writer(&mut self.durable_log.as_mut().unwrap(), &records)
                    .unwrap();
            }
            BaseDurabilityLevel::SyncImmediately => {
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
        let us = self.us.expect("on_input should never be called before on_commit");

        if self.durable_log.is_none() {
            let now = time::now();
            let today = time::strftime("%F", &now).unwrap();
            //let log_filename = format!("/tmp/soup-log-{}-{:?}-{}.json",
            //                           today, self.global_address.unwrap(), self.unique_id);
            let log_filename = format!("/tmp/soup-log-{}-{}-{}.json",
                                       today, us, self.unique_id);
            self.durable_log_path = Some(PathBuf::from(&log_filename));
            let path = self.durable_log_path.as_ref().unwrap().as_path();

            // TODO(jmftrindade): Current semantics is to overwrite an existing log. Once we
            // have recovery code, we obviously do not want to overwrite this log before recovering.
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

            match self.durability {
                BaseDurabilityLevel::None => {
                    self.durable_log = None;
                    println!("SET DURABLE LOG TO NONE, BECAUSE DURABILITY IS NONE!");
                },

                // TODO(jmftrindade): Use our own flush strategy instead?
                BaseDurabilityLevel::Buffered => {
                    self.durable_log = Some(BufWriter::with_capacity_and_strategy(
                        LOG_BUFFER_CAPACITY, file, WhenFull))
                },

                // XXX(jmftrindade): buf_redux does not provide a "sync immediately" flush strategy
                // out of the box, so we handle that from persist_to_log by dropping sync_data.
                BaseDurabilityLevel::SyncImmediately => {
                    self.durable_log = Some(BufWriter::with_capacity_and_strategy(
                        LOG_BUFFER_CAPACITY, file, WhenFull))
                }
            }

            println!("SET BASE DURABLE_LOG TO SOME!");
        }
    }
}

/// A Base clone must have a different unique_id so that no two copies write to the same file.
/// Resetting the writer to None in the original copy is not enough to guarantee that, as the
/// original object can still re-open the log file on-demand from Base::persist_to_log.
impl Clone for Base {
    fn clone(&self) -> Base {
        Base {
            durability: self.durability,
            durable_log: None,
            durable_log_path: None,
            global_address: self.global_address,
            key_column: self.key_column,
            unique_id: ProcessUniqueId::new(),
            us: self.us,
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            durability: BaseDurabilityLevel::Buffered,
            durable_log: None,
            durable_log_path: None,
            global_address: None,
            key_column: None,
            unique_id: ProcessUniqueId::new(),
            us: None,
        }
    }
}

#[cfg(test)]
impl Drop for Base {
    fn drop(&mut self) {
        println!("Dropping Base!");

        if self.durable_log.is_some() {
            println!("Durable log is SOME!");
        } else {
            println!("Durable log is NONE!");
        }

        if self.durable_log_path.is_some() {
            fs::remove_file(self.durable_log_path.as_ref().unwrap().as_path()).unwrap();

            // Somehow this is never reached.
            panic!("Removed file!");
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
        !materialized && self.key_column.is_some()
    }

    fn on_connected(&mut self, _: &Graph) {}
    fn on_commit(&mut self, us: NodeAddress, _: &HashMap<NodeAddress, NodeAddress>) {
        self.us = Some(us);
    }
    fn on_input(&mut self,
                _: NodeAddress,
                rs: Records,
                _: &DomainNodes,
                state: &StateMap)
                -> Records {
        // Write incoming records to log before processing them if we are a durable node.
        match self.durability {
            BaseDurabilityLevel::Buffered |
            BaseDurabilityLevel::SyncImmediately => self.persist_to_log(&rs),
            BaseDurabilityLevel::None => (),
        }

        rs.into_iter()
            .map(|r| match r {
                Record::Positive(u) => Record::Positive(u),
                Record::Negative(u) => Record::Negative(u),
                Record::DeleteRequest(key) => {
                    let col = self.key_column
                        .expect("base must have a key column to support deletions");
                    let db = state.get(self.us.as_ref().unwrap().as_local())
                        .expect("base must have its own state materialized to support deletions");
                    let rows = db.lookup(col, &key);
                    assert_eq!(rows.len(), 1);

                    Record::Negative(rows[0].clone())
                }
            })
            .collect()
    }

    fn suggest_indexes(&self, n: NodeAddress) -> HashMap<NodeAddress, usize> {
        if self.key_column.is_some() {
            Some((n, self.key_column.unwrap())).into_iter().collect()
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

    /// A base node must remember its own global address so that it can use it as an id for durable
    /// log.
    fn set_global_address(&mut self, n: NodeAddress) {
        self.global_address = Some(n);
    }
}
