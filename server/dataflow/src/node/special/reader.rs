use crate::backlog;
use crate::prelude::*;
use noria::channel;

/// A StreamUpdate reflects the addition or deletion of a row from a reader node.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamUpdate {
    /// Indicates the addition of a new row
    AddRow(Vec<DataType>),
    /// Indicates the removal of an existing row
    DeleteRow(Vec<DataType>),
}

impl From<Record> for StreamUpdate {
    fn from(other: Record) -> Self {
        match other {
            Record::Positive(u) => StreamUpdate::AddRow(u),
            Record::Negative(u) => StreamUpdate::DeleteRow(u),
        }
    }
}

impl From<Vec<DataType>> for StreamUpdate {
    fn from(other: Vec<DataType>) -> Self {
        StreamUpdate::AddRow(other)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Reader {
    #[serde(skip)]
    writer: Option<backlog::WriteHandle>,

    #[serde(skip)]
    streamers: Vec<channel::StreamSender<Vec<StreamUpdate>>>,

    for_node: NodeIndex,
    state: Option<Vec<usize>>,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        assert!(self.writer.is_none());
        Reader {
            writer: None,
            streamers: self.streamers.clone(),
            state: self.state.clone(),
            for_node: self.for_node,
        }
    }
}

impl Reader {
    pub fn new(for_node: NodeIndex) -> Self {
        Reader {
            writer: None,
            streamers: Vec::new(),
            state: None,
            for_node,
        }
    }

    pub fn shard(&mut self, _: usize) {}

    pub fn is_for(&self) -> NodeIndex {
        self.for_node
    }

    #[allow(dead_code)]
    fn writer(&self) -> Option<&backlog::WriteHandle> {
        self.writer.as_ref()
    }

    pub(crate) fn writer_mut(&mut self) -> Option<&mut backlog::WriteHandle> {
        self.writer.as_mut()
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        use std::mem;
        Self {
            writer: self.writer.take(),
            streamers: mem::replace(&mut self.streamers, Vec::new()),
            state: self.state.clone(),
            for_node: self.for_node,
        }
    }

    pub(crate) fn add_streamer(
        &mut self,
        new_streamer: channel::StreamSender<Vec<StreamUpdate>>,
    ) -> Result<(), channel::StreamSender<Vec<StreamUpdate>>> {
        self.streamers.push(new_streamer);
        Ok(())
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

    pub(crate) fn state_size(&self) -> Option<u64> {
        self.writer.as_ref().map(SizeOf::deep_size_of)
    }

    /// Evict a randomly selected key, returning the number of bytes evicted.
    /// Note that due to how `evmap` applies the evictions asynchronously, we can only evict a
    /// single key at a time here.
    pub(crate) fn evict_random_key(&mut self) -> u64 {
        let mut bytes_freed = 0;
        if let Some(ref mut handle) = self.writer {
            let mut rng = rand::thread_rng();
            bytes_freed = handle.evict_random_key(&mut rng);
            handle.swap();
        }
        bytes_freed
    }

    pub(in crate::node) fn on_eviction(&mut self, keys: &[Vec<DataType>]) {
        // NOTE: *could* be None if reader has been created but its state hasn't been built yet
        if let Some(w) = self.writer.as_mut() {
            for k in keys {
                w.mut_with_key(&k[..]).mark_hole();
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
                            Ok((None, _)) => {
                                // row would miss in partial state.
                                // leave it blank so later lookup triggers replay.
                                false
                            }
                            Err(_) => unreachable!(),
                            _ => {
                                // state is already present,
                                // so we can safely keep it up to date.
                                true
                            }
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
                            Ok((None, _)) => {
                                // filling a hole with replay -- ok
                                true
                            }
                            Ok((Some(_), _)) => {
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

            if self.streamers.is_empty() {
                state.add(m.take_data());
            } else {
                state.add(m.data().iter().cloned());
            }

            if swap {
                // TODO: avoid doing the pointer swap if we didn't modify anything (inc. ts)
                state.swap();
            }
        }

        // TODO: don't send replays to streams?

        if !self.streamers.is_empty() {
            let mut data = Some(m.take().unwrap().take_data()); // so we can .take() for last tx
            let mut left = self.streamers.len();

            // remove any channels where the receiver has hung up
            self.streamers.retain(|tx| {
                left -= 1;
                if left == 0 {
                    tx.send(data.take().unwrap().into_iter().map(Into::into).collect())
                } else {
                    tx.send(data.clone().unwrap().into_iter().map(Into::into).collect())
                }
                .is_ok()
            });
        }
    }
}
