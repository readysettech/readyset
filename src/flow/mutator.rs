use flow;
use flow::prelude::*;
use checktable;
use channel::TransactionReplySender;

use vec_map::VecMap;

use std::sync::mpsc;
use std::thread;
use std::time;

/// Indicates why a Mutator operation failed.
#[derive(Serialize, Deserialize, Debug)]
pub enum MutatorError {
    /// Incorrect number of columns specified for operations: (expected, got).
    WrongColumnCount(usize, usize),
    /// Transaction was unable to complete due to a conflict.
    TransactionFailed,
}

/// A `Mutator` is used to perform reads and writes to base nodes.
pub struct Mutator {
    pub(crate) tx: flow::domain::DomainInputHandle,
    pub(crate) addr: LocalNodeIndex,
    pub(crate) key_is_primary: bool,
    pub(crate) key: Vec<usize>,
    pub(crate) tx_reply_channel: (
        TransactionReplySender<Result<i64, ()>>,
        mpsc::Receiver<Result<i64, ()>>,
    ),
    pub(crate) transactional: bool,
    pub(crate) dropped: VecMap<DataType>,
    pub(crate) tracer: Tracer,
    pub(crate) expected_columns: usize,
}

impl Clone for Mutator {
    fn clone(&self) -> Self {
        let reply_chan = mpsc::channel();
        let reply_chan = (
            TransactionReplySender::from_local(reply_chan.0),
            reply_chan.1,
        );

        Self {
            tx: self.tx.clone(),
            addr: self.addr.clone(),
            key: self.key.clone(),
            key_is_primary: self.key_is_primary.clone(),
            tx_reply_channel: reply_chan,
            transactional: self.transactional,
            dropped: self.dropped.clone(),
            tracer: None,
            expected_columns: self.expected_columns,
        }
    }
}

impl Mutator {
    fn inject_dropped_cols(&self, rs: &mut Records) {
        let ndropped = self.dropped.len();
        if ndropped != 0 {
            // inject defaults for dropped columns
            let dropped = self.dropped.iter().rev();
            for r in rs.iter_mut() {
                use std::mem;

                // get a handle to the underlying data vector
                let r = match *r {
                    Record::Positive(ref mut r) |
                    Record::Negative(ref mut r) => r,
                    _ => continue,
                };

                // we want to iterate over all the dropped columns
                let dropped = dropped.clone();

                // we want to be a bit careful here to avoid shifting elements multiple times. we
                // do this by moving from the back, and swapping the tail element to the end of the
                // vector until we hit each index.

                // make room in the record
                r.reserve(ndropped);
                let mut free = r.len() + ndropped;
                let mut last_unmoved = r.len() - 1;
                unsafe { r.set_len(free) };
                // *technically* we should set all the extra elements just in case we get a panic
                // below, because otherwise we might call drop() on uninitialized memory. we would
                // do that like this (note the ::forget() on the swapped-out value):
                //
                //   for i in (free - ndropped)..r.len() {
                //       mem::forget(mem::replace(r.get_mut(i).unwrap(), DataType::None));
                //   }
                //
                // but for efficiency we dont' do that, and just make sure that the code below
                // doesn't panic. what's the worst that could happen, right?

                // keep trying to insert the next dropped column
                'next: for (next_insert, default) in dropped {
                    // think of this being at the bottom of the loop
                    // we just hoist it here to avoid underflow if we ever insert at 0
                    free -= 1;

                    // shift elements until the next free slot is the one we want to insert into
                    while free > next_insert {
                        // shift another element so we the free slot is at a lower index
                        r.swap(last_unmoved, free);
                        free -= 1;

                        if last_unmoved == 0 {
                            // avoid underflow
                            debug_assert_eq!(next_insert, free);
                            break;
                        }
                        last_unmoved -= 1;
                    }

                    // we're at the right index -- insert the dropped value
                    let current = r.get_mut(next_insert).unwrap();
                    let old = mem::replace(current, default.clone());
                    // the old value is uninitialized memory!
                    // (remember how we called set_len above?)
                    mem::forget(old);

                    // here, I'll help:
                    // free -= 1;
                }
            }
        }
    }

    fn send(&mut self, mut rs: Records) {
        self.inject_dropped_cols(&mut rs);
        let m = if self.transactional {
            box Packet::Transaction {
                link: Link::new(self.addr, self.addr),
                data: rs,
                state: TransactionState::WillCommit,
                tracer: self.tracer.clone(),
            }
        } else {
            box Packet::Message {
                link: Link::new(self.addr, self.addr),
                data: rs,
                tracer: self.tracer.clone(),
            }
        };

        self.tx.base_send_spin(m, &self.key[..]).unwrap();
    }

    fn tx_send(&mut self, mut rs: Records, t: checktable::Token) -> Result<i64, ()> {
        assert!(self.transactional);

        self.inject_dropped_cols(&mut rs);
        let send = self.tx_reply_channel.0.clone();
        let m = box Packet::Transaction {
            link: Link::new(self.addr, self.addr),
            data: rs,
            state: TransactionState::Pending(t, send),
            tracer: self.tracer.clone(),
        };
        self.tx.base_send(m, &self.key[..]).unwrap();
        loop {
            match self.tx_reply_channel.1.try_recv() {
                Ok(r) => return r,
                Err(mpsc::TryRecvError::Empty) => thread::yield_now(),
                Err(mpsc::TryRecvError::Disconnected) => unreachable!(),
            }
        }
    }

    /// Perform a non-transactional write to the base node this Mutator was generated for.
    pub fn put<V>(&mut self, u: V) -> Result<(), MutatorError>
    where
        V: Into<Vec<DataType>>,
    {
        let data = vec![u.into()];
        if data[0].len() != self.expected_columns {
            return Err(MutatorError::WrongColumnCount(
                self.expected_columns,
                data[0].len(),
            ));
        }

        Ok(self.send(data.into()))
    }

    /// Perform a transactional write to the base node this Mutator was generated for.
    pub fn transactional_put<V>(&mut self, u: V, t: checktable::Token) -> Result<i64, MutatorError>
    where
        V: Into<Vec<DataType>>,
    {
        let data = vec![u.into()];
        if data[0].len() != self.expected_columns {
            return Err(MutatorError::WrongColumnCount(
                self.expected_columns,
                data[0].len(),
            ));
        }

        self.tx_send(data.into(), t).map_err(|()|MutatorError::TransactionFailed)
    }

    /// Perform a non-transactional delete from the base node this Mutator was generated for.
    pub fn delete<I>(&mut self, key: I) -> Result<(), MutatorError>
    where
        I: Into<Vec<DataType>>,
    {
        Ok(self.send(vec![Record::DeleteRequest(key.into())].into()))
    }

    /// Perform a transactional delete from the base node this Mutator was generated for.
    pub fn transactional_delete<I>(
        &mut self,
        key: I,
        t: checktable::Token,
    ) -> Result<i64, MutatorError>
    where
        I: Into<Vec<DataType>>,
    {
        self.tx_send(vec![Record::DeleteRequest(key.into())].into(), t)
            .map_err(|()|MutatorError::TransactionFailed)
    }

    /// Perform a non-transactional update (delete followed by put) to the base node this Mutator
    /// was generated for.
    pub fn update<V>(&mut self, u: V) -> Result<(), MutatorError>
    where
        V: Into<Vec<DataType>>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        let u = u.into();
        if u.len() != self.expected_columns {
            return Err(MutatorError::WrongColumnCount(
                self.expected_columns,
                u.len(),
            ));
        }

        let pkey = self.key.iter().map(|&col| &u[col]).cloned().collect();
        Ok(self.send(
            vec![Record::DeleteRequest(pkey), u.into()].into(),
        ))
    }

    /// Perform a transactional update (delete followed by put) to the base node this Mutator was
    /// generated for.
    pub fn transactional_update<V>(
        &mut self,
        u: V,
        t: checktable::Token,
    ) -> Result<i64, MutatorError>
    where
        V: Into<Vec<DataType>>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        let u: Vec<_> = u.into();
        if u.len() != self.expected_columns {
            return Err(MutatorError::WrongColumnCount(
                self.expected_columns,
                u.len(),
            ));
        }

        let m = vec![
            Record::DeleteRequest(self.key.iter().map(|&col| &u[col]).cloned().collect()),
            u.into(),
        ].into();
        self.tx_send(m, t).map_err(|()|MutatorError::TransactionFailed)
    }

    /// Attach a tracer to all packets sent until `stop_tracing` is called. The tracer will cause
    /// events to be sent to the returned Receiver indicating the progress of the packet through the
    /// graph.
    pub fn start_tracing(&mut self) -> mpsc::Receiver<(time::Instant, PacketEvent)> {
        let (tx, rx) = mpsc::channel();
        self.tracer = Some(vec![TransactionReplySender::from_local(tx)]);
        rx
    }

    /// Stop attaching the tracer to packets sent.
    pub fn stop_tracing(&mut self) {
        self.tracer = None;
    }
}
