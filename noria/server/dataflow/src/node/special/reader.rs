use noria::{consistency::Timestamp, KeyComparison};

use crate::backlog;
use crate::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct Reader {
    #[serde(skip)]
    writer: Option<backlog::WriteHandle>,

    for_node: NodeIndex,
    state: Option<Vec<usize>>,
    pub operator: nom_sql::BinaryOperator,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        assert!(self.writer.is_none());
        Reader {
            writer: None,
            state: self.state.clone(),
            for_node: self.for_node,
            operator: self.operator,
        }
    }
}

impl Reader {
    pub fn new(for_node: NodeIndex, operator: nom_sql::BinaryOperator) -> Self {
        Reader {
            writer: None,
            state: None,
            for_node,
            operator,
        }
    }

    pub fn shard(&mut self, _: usize) {}

    pub fn is_for(&self) -> NodeIndex {
        self.for_node
    }

    pub(crate) fn writer(&self) -> Option<&backlog::WriteHandle> {
        self.writer.as_ref()
    }

    pub(crate) fn writer_mut(&mut self) -> Option<&mut backlog::WriteHandle> {
        self.writer.as_mut()
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        Self {
            writer: self.writer.take(),
            state: self.state.clone(),
            for_node: self.for_node,
            operator: self.operator.clone(),
        }
    }

    pub fn is_materialized(&self) -> bool {
        self.state.is_some()
    }

    pub(crate) fn is_partial(&self) -> bool {
        match self.writer {
            None => false,
            Some(ref state) => state.is_partial(),
        }
    }

    pub(crate) fn set_write_handle(&mut self, wh: backlog::WriteHandle) {
        assert!(self.writer.is_none());
        self.writer = Some(wh);
    }

    pub fn key(&self) -> Option<&[usize]> {
        self.state.as_ref().map(|s| &s[..])
    }

    pub fn set_key(&mut self, key: &[usize]) {
        if let Some(ref skey) = self.state {
            assert_eq!(&skey[..], key);
        } else {
            self.state = Some(Vec::from(key));
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.writer.as_ref().map(|w| w.is_empty()).unwrap_or(true)
    }

    pub(crate) fn state_size(&self) -> Option<u64> {
        self.writer.as_ref().map(SizeOf::deep_size_of)
    }

    /// Evict a randomly selected key, returning the number of bytes evicted.
    /// Note that due to how `reader_map` applies the evictions asynchronously, we can only evict a
    /// single key at a time here.
    pub(crate) fn evict_random_keys(&mut self, n: usize) -> u64 {
        let mut bytes_freed = 0;
        if let Some(ref mut handle) = self.writer {
            let mut rng = rand::thread_rng();
            bytes_freed = handle.evict_random_keys(&mut rng, n);
            handle.swap();
        }
        bytes_freed
    }

    pub(in crate::node) fn on_eviction(&mut self, keys: &[KeyComparison]) {
        // NOTE: *could* be None if reader has been created but its state hasn't been built yet
        if let Some(w) = self.writer.as_mut() {
            for k in keys {
                w.mark_hole(k);
            }
            w.swap();
        }
    }

    pub(in crate::node) fn process(&mut self, m: &mut Option<Box<Packet>>, swap: bool) {
        if let Some(ref mut state) = self.writer {
            let m = m.as_mut().unwrap();
            // make sure we don't fill a partial materialization
            // hole with incomplete (i.e., non-replay) state.
            if m.is_regular() && state.is_partial() {
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.entry_from_record(&row[..]).try_find_and(|_| ()) {
                            Err(e) if e.is_miss() => {
                                // row would miss in partial state.
                                // leave it blank so later lookup triggers replay.
                                false
                            }
                            Ok(_) => {
                                // state is already present,
                                // so we can safely keep it up to date.
                                true
                            }
                            Err(_) => unreachable!(),
                        }
                    });
                });
            }

            // it *can* happen that multiple readers miss (and thus request replay for) the
            // same hole at the same time. we need to make sure that we ignore any such
            // duplicated replay.
            if !m.is_regular() && state.is_partial() {
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.entry_from_record(&row[..]).try_find_and(|_| ()) {
                            Err(e) if e.is_miss() => {
                                // filling a hole with replay -- ok
                                true
                            }
                            Ok(_) => {
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
                });
            }

            state.add(m.take_data());

            if swap {
                // TODO: avoid doing the pointer swap if we didn't modify anything (inc. ts)
                state.swap();
            }
        }
    }

    pub(in crate::node) fn process_timestamp(&mut self, m: Timestamp) {
        if let Some(ref mut handle) = self.writer {
            handle.set_timestamp(m);

            // Ensure the write is published.
            handle.swap();
        }
    }
}
