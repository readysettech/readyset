use std::time::SystemTime;

use dataflow_expression::ReaderProcessing;
use failpoint_macros::failpoint;
use metrics::histogram;
use readyset_client::metrics::recorded;
use readyset_client::{KeyColumnIdx, ViewPlaceholder};
use serde::{Deserialize, Serialize};
use tracing::{trace, warn};

use crate::backlog;
use crate::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct Reader {
    for_node: NodeIndex,
    index: Option<Index>,

    /// Operations to perform on the result set after the rows are returned from the lookup
    reader_processing: ReaderProcessing,

    /// Vector of (placeholder_number, key_column_index). The placeholder_number corresponds to
    /// where the placeholder appears in the SQL query and the key_column_index corresponds to the
    /// key column index in the reader state.
    ///
    /// The data is stored in this manner instead of in a Hashmap to support ordered iteration.
    placeholder_map: Vec<(ViewPlaceholder, KeyColumnIdx)>,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Reader {
            for_node: self.for_node,
            reader_processing: self.reader_processing.clone(),
            index: self.index.clone(),
            placeholder_map: self.placeholder_map.clone(),
        }
    }
}

impl Reader {
    pub fn new(for_node: NodeIndex, reader_processing: ReaderProcessing) -> Self {
        Reader {
            for_node,
            reader_processing,
            index: None,
            placeholder_map: Default::default(),
        }
    }

    pub fn shard(&mut self, _: usize) {}

    pub fn is_for(&self) -> NodeIndex {
        self.for_node
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        Self {
            for_node: self.for_node,
            reader_processing: self.reader_processing.clone(),
            index: self.index.clone(),
            placeholder_map: self.placeholder_map.clone(),
        }
    }

    pub fn is_materialized(&self) -> bool {
        self.index.is_some()
    }

    pub fn index(&self) -> Option<&Index> {
        self.index.as_ref()
    }

    pub fn key(&self) -> Option<&[usize]> {
        self.index.as_ref().map(|s| &s.columns[..])
    }

    pub fn index_type(&self) -> Option<IndexType> {
        self.index.as_ref().map(|index| index.index_type)
    }

    #[must_use]
    pub fn with_index(mut self, index: &Index) -> Self {
        self.set_index(index);
        self
    }

    pub fn set_index(&mut self, index: &Index) {
        if let Some(ref m_index) = self.index {
            debug_assert_eq!(m_index, index);
        } else {
            self.index = Some(index.clone());
        }
    }

    /// Sets the placeholder to column mapping if it is not already set.
    ///
    /// We do not currently support multiple mappings from placeholders to key columns. That would
    /// require a method for resolving which mapping should be used for each query.
    ///
    /// This method will need to be implemented before using the same reader for functionally
    /// identical queries with different parameter orderings (e.g., 'SELECT * FROM t WHERE a = ?
    /// AND b = ?' and 'SELECT * FROM t WHERE b = ? AND a = ?')
    pub fn set_mapping(&mut self, mapping: Vec<(ViewPlaceholder, KeyColumnIdx)>) {
        if !self.placeholder_map.is_empty() {
            debug_assert_eq!(self.placeholder_map, mapping);
        } else {
            self.placeholder_map = mapping
        }
    }

    /// Returns the mapping from placeholder to reader key column. There is exactly one value for
    /// each reader key column in the map
    pub fn mapping(&self) -> &[(ViewPlaceholder, KeyColumnIdx)] {
        self.placeholder_map.as_ref()
    }

    #[allow(clippy::unreachable)]
    #[failpoint("reader-handle-packet")]
    pub(in crate::node) fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        swap: bool,
        state: &mut backlog::WriteHandle,
    ) {
        let m = m.as_mut().unwrap();
        m.handle_trace(
            |trace| match SystemTime::now().duration_since(trace.start) {
                Ok(d) => {
                    histogram!(
                        recorded::PACKET_WRITE_PROPAGATION_TIME,
                        d.as_micros() as f64
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Write latency trace failed");
                }
            },
        );
        // make sure we don't fill a partial materialization
        // hole with incomplete (i.e., non-replay) state.
        if m.is_regular() && state.is_partial() {
            let data = m.mut_data();
            trace!(?data, "reader received regular message");
            data.retain(|row| {
                match state.contains_record(&row[..]) {
                    Ok(false) => {
                        // row would miss in partial state.
                        // leave it blank so later lookup triggers replay.
                        trace!(?row, "dropping row that hit partial hole");
                        false
                    }
                    Ok(true) => {
                        // state is already present,
                        // so we can safely keep it up to date.
                        true
                    }
                    Err(reader_map::Error::NotPublished) => {
                        // If we got here it means we got a `NotReady` error type. This is
                        // impossible, because when readers are instantiated we issue a
                        // commit to the underlying map, which makes it Ready.
                        unreachable!(
                            "somehow found a NotReady reader even though we've
                                    already initialized it with a commit"
                        )
                    }
                    Err(reader_map::Error::Destroyed) => {
                        unreachable!("somehow map was destroyed but we hold a mutable reference")
                    }
                }
            });
        }

        // it *can* happen that multiple readers miss (and thus request replay for) the
        // same hole at the same time. we need to make sure that we ignore any such
        // duplicated replay.
        if !m.is_regular() && state.is_partial() {
            let data = m.mut_data();
            trace!(?data, "reader received replay");
            data.retain(|row| {
                match state.contains_record(&row[..]) {
                    Ok(false) => {
                        // filling a hole with replay -- ok
                        true
                    }
                    Ok(true) => {
                        trace!(?row, "reader dropping row that hit already-filled hole");
                        // a given key should only be replayed to once!
                        false
                    }
                    Err(_) => {
                        // state has not yet been swapped, which means it's new,
                        // which means there are no readers, which means no
                        // requests for replays have been issued by readers, which
                        // means no duplicates can be received.
                        true
                    }
                }
            });
        }

        state.add(m.take_data());

        if swap {
            // TODO: avoid doing the pointer swap if we didn't modify anything (inc. ts)
            state.swap();
        }
    }

    /// Get a reference to the reader's post lookup.
    pub fn reader_processing(&self) -> &ReaderProcessing {
        &self.reader_processing
    }
}
