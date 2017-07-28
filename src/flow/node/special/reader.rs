use backlog;
use channel;
use checktable;
use flow::prelude::*;

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
            Record::DeleteRequest(..) => unreachable!(),
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
    streamers: Option<Vec<channel::StreamSender<Vec<StreamUpdate>>>>,

    #[serde(skip)]
    token_generator: Option<checktable::TokenGenerator>,

    for_node: NodeIndex,
    state: Option<usize>,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        assert!(self.writer.is_none());
        assert!(
            self.streamers
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        );
        Reader {
            writer: None,
            streamers: self.streamers.clone(),
            state: self.state.clone(),
            token_generator: self.token_generator.clone(),
            for_node: self.for_node,
        }
    }
}

impl Reader {
    pub fn new(for_node: NodeIndex) -> Self {
        Reader {
            writer: None,
            streamers: Some(Vec::new()),
            state: None,
            token_generator: None,
            for_node,
        }
    }

    pub fn shard(&mut self, _: usize) {}

    pub fn is_for(&self) -> NodeIndex {
        self.for_node
    }

    pub fn writer(&self) -> Option<&backlog::WriteHandle> {
        self.writer.as_ref()
    }

    pub fn writer_mut(&mut self) -> Option<&mut backlog::WriteHandle> {
        self.writer.as_mut()
    }

    pub fn take(&mut self) -> Self {
        Self {
            writer: self.writer.take(),
            streamers: self.streamers.take(),
            state: self.state.clone(),
            token_generator: self.token_generator.clone(),
            for_node: self.for_node,
        }
    }

    pub fn add_streamer(
        &mut self,
        new_streamer: channel::StreamSender<Vec<StreamUpdate>>,
    ) -> Result<(), channel::StreamSender<Vec<StreamUpdate>>> {
        if let Some(ref mut streamers) = self.streamers {
            streamers.push(new_streamer);
            Ok(())
        } else {
            Err(new_streamer)
        }
    }

    pub fn is_materialized(&self) -> bool {
        self.state.is_some()
    }

    pub fn set_write_handle(&mut self, wh: backlog::WriteHandle) {
        assert!(self.writer.is_none());
        self.writer = Some(wh);
    }

    pub fn key(&self) -> Option<usize> {
        self.state
    }

    pub fn set_key(&mut self, key: usize) {
        if let Some(skey) = self.state {
            assert_eq!(skey, key);
        } else {
            self.state = Some(key);
        }
    }

    pub fn token_generator(&self) -> Option<&checktable::TokenGenerator> {
        self.token_generator.as_ref()
    }

    pub fn set_token_generator(&mut self, gen: checktable::TokenGenerator) {
        assert!(self.token_generator.is_none());
        self.token_generator = Some(gen);
    }

    pub fn process(&mut self, m: &mut Option<Box<Packet>>, swap: bool) {
        if let Some(ref mut state) = self.writer {
            let m = m.as_mut().unwrap();
            // make sure we don't fill a partial materialization
            // hole with incomplete (i.e., non-replay) state.
            if m.is_regular() && state.is_partial() {
                let key = state.key();
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.try_find_and(&row[key], |_| ()) {
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
                let key = state.key();
                m.map_data(|data| {
                    data.retain(|row| {
                        match state.try_find_and(&row[key], |_| ()) {
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

            if self.streamers.as_ref().unwrap().is_empty() {
                state.add(m.take_data());
            } else {
                state.add(m.data().iter().cloned());
            }
            if let Packet::Transaction {
                state: TransactionState::Committed(ts, ..),
                ..
            } = **m
            {
                state.update_ts(ts);
            }

            if swap {
                // TODO: avoid doing the pointer swap if we didn't modify anything (inc. ts)
                state.swap();
            }
        }

        // TODO: don't send replays to streams?

        m.as_mut().unwrap().trace(PacketEvent::ReachedReader);

        if !self.streamers.as_ref().unwrap().is_empty() {
            let mut data = Some(m.take().unwrap().take_data()); // so we can .take() for last tx
            let mut left = self.streamers.as_ref().unwrap().len();

            // remove any channels where the receiver has hung up
            self.streamers.as_mut().unwrap().retain(|tx| {
                left -= 1;
                if left == 0 {
                    tx.send(data.take().unwrap().into_iter().map(|r| r.into()).collect())
                } else {
                    tx.send(
                        data.clone()
                            .unwrap()
                            .into_iter()
                            .map(|r| r.into())
                            .collect(),
                    )
                }.is_ok()
            });
        }
    }
}
