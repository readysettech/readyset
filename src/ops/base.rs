// TODO(jmftrindade): Put the writing to disk behind a runtime command-line flag.
use serde_json;
use snowflake::ProcessUniqueId;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::Path;

/// Base is used to represent the root nodes of the distributary data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug)]
pub struct Base {
    primary_key: Option<Vec<usize>>,
    persistent_log_writer: Option<BufWriter<File>>,
    us: Option<NodeAddress>,

    // This id is unique within the same process.
    //
    // TODO(jmftrindade): Figure out the story here.  While ProcessUniqueId is guaranteed to be
    // unique within the same process, the assignment of ids is not deterministic across multiple
    // process runs. This is just a tuple of 2 monotonically increasing counters: the first is per
    // process, and the second is "within" that process.
    unique_id: ProcessUniqueId,
}

impl Base {
    /// Create a base node operator.
    pub fn new(primary_key: Vec<usize>) -> Self {
        Base {
            primary_key: Some(primary_key),
            persistent_log_writer: None,
            us: None,
            unique_id: ProcessUniqueId::new(),
        }
    }

    /// Write records to persistent log.
    fn persist_to_log(&mut self, records: &Records) {
        self.ensure_log_writer();
        serde_json::to_writer(&mut self.persistent_log_writer.as_mut().unwrap(), &records).unwrap();
    }

    /// Open persistent log and initialize a buffered writer to it if successful.
    fn ensure_log_writer(&mut self) {
        let us = self.us.expect("on_input should never be called before on_commit");
        ;

        if self.persistent_log_writer.is_none() {
            // Check whether NodeAddress is global or local?
            let log_filename = format!("/tmp/soup-{}-{}.json", us, self.unique_id);
            let path = Path::new(&log_filename);

            // TODO(jmftrindade): Current semantics is to overwrite an existing log. Once we
            // have recovery code, we obviously do not want to overwrite this log before recovering.
            let file = match OpenOptions::new()
                .read(false)
                .append(false)
                .write(true)
                .create(true)
                .open(path) {
                Err(reason) => panic!("Unable to open persistent log file {}, reason: {}",
                                      path.display(), reason),
                Ok(file) => file,
            };
            self.persistent_log_writer = Some(BufWriter::new(file));
        }
    }
}

/// A Base clone must have a different unique_id so that no two copies write to the same file.
/// Resetting the writer to None in the original copy is not enough to guarantee that, as the
/// original object can still re-open the log file on-demand from Base::persist_to_log.
impl Clone for Base {
    fn clone(&self) -> Base {
        Base {
            key_column: self.key_column,
            persistent_log_writer: None,
            unique_id: ProcessUniqueId::new(),
            us: self.us,
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            primary_key: None,
            us: None,
            unique_id: ProcessUniqueId::new(),
            persistent_log_writer: None,
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
                rs: Records,
                _: &DomainNodes,
                state: &StateMap)
                -> Records {

        // Write incoming records to log before processing them.
        self.persist_to_log(&rs);

        rs.into_iter()
            .map(|r| match r {
                Record::Positive(u) => Record::Positive(u),
                Record::Negative(u) => Record::Negative(u),
                Record::DeleteRequest(key) => {
                    let cols = self.primary_key
                        .as_ref()
                        .expect("base must have a primary key to support deletions");
                    let db = state.get(self.us.as_ref().unwrap().as_local())
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
            Some((n, self.primary_key.as_ref().unwrap().clone())).into_iter().collect()
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
