use flow::prelude::*;
use checktable;

use vec_map::VecMap;

use std::sync::mpsc;
use std::thread;
use std::time;

/// A `Mutator` is used to perform reads and writes to base nodes.
pub struct Mutator {
    pub(crate) src: NodeAddress,
    pub(crate) tx: mpsc::SyncSender<Box<Packet>>,
    pub(crate) addr: NodeAddress,
    pub(crate) primary_key: Vec<usize>,
    pub(crate) tx_reply_channel: (mpsc::Sender<Result<i64, ()>>, mpsc::Receiver<Result<i64, ()>>),
    pub(crate) transactional: bool,
    pub(crate) dropped: VecMap<DataType>,
    pub(crate) tracer: Tracer,
}

impl Clone for Mutator {
    fn clone(&self) -> Self {
        Self {
            src: self.src.clone(),
            tx: self.tx.clone(),
            addr: self.addr.clone(),
            primary_key: self.primary_key.clone(),
            tx_reply_channel: mpsc::channel(),
            transactional: self.transactional,
            dropped: self.dropped.clone(),
            tracer: None,
        }
    }
}

impl Mutator {
    fn inject_dropped_cols(&self, rs: &mut Records) {
        // NOTE: this is pretty expensive until https://github.com/contain-rs/vec-map/pull/33 lands
        let ndropped = self.dropped.len();
        if ndropped != 0 {
            // inject defaults for dropped columns
            let dropped = self.dropped.iter().rev();
            for r in rs.iter_mut() {
                // get a handle to the underlying data vector
                use std::sync::Arc;
                let v = match *r {
                    Record::Positive(ref mut v) |
                    Record::Negative(ref mut v) => v,
                    _ => continue,
                };
                let r = Arc::get_mut(v).expect("send should have complete ownership of records");

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

                // keep trying to insert the next dropped column
                'next: for (next_insert, default) in dropped {
                    // think of this being at the bottom of the loop
                    // we just hoist it here to avoid underflow if we ever insert at 0
                    free -= 1;

                    // the next free slot is not one we want to insert into
                    // keep shifting elements until it is
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
                    *r.get_mut(next_insert).unwrap() = default.clone();

                    // here, I'll help:
                    // free -= 1;
                }
            }
        }
    }

    fn send(&self, mut rs: Records) {
        self.inject_dropped_cols(&mut rs);
        let m = if self.transactional {
            box Packet::Transaction {
                    link: Link::new(self.src, self.addr),
                    data: rs,
                    state: TransactionState::WillCommit,
                    tracer: self.tracer.clone(),
                }
        } else {
            box Packet::Message {
                    link: Link::new(self.src, self.addr),
                    data: rs,
                    tracer: self.tracer.clone(),
                }
        };

        self.tx.clone().send(m).unwrap();
    }

    fn tx_send(&self, mut rs: Records, t: checktable::Token) -> Result<i64, ()> {
        assert!(self.transactional);

        self.inject_dropped_cols(&mut rs);
        let send = self.tx_reply_channel.0.clone();
        let m = box Packet::Transaction {
                        link: Link::new(self.src, self.addr),
                        data: rs,
                        state: TransactionState::Pending(t, send),
                        tracer: self.tracer.clone(),
                    };
        self.tx.clone().send(m).unwrap();
        loop {
            match self.tx_reply_channel.1.try_recv() {
                Ok(r) => return r,
                Err(..) => thread::yield_now(),
            }
        }
    }

    /// Perform a non-transactional write to the base node this Mutator was generated for.
    pub fn put<V>(&self, u: V)
        where V: Into<Vec<DataType>>
    {
        self.send(vec![u.into()].into())
    }

    /// Perform a transactional write to the base node this Mutator was generated for.
    pub fn transactional_put<V>(&self, u: V, t: checktable::Token) -> Result<i64, ()>
        where V: Into<Vec<DataType>>
    {
        self.tx_send(vec![u.into()].into(), t)
    }

    /// Perform a non-transactional delete frome the base node this Mutator was generated for.
    pub fn delete<I>(&self, key: I)
        where I: Into<Vec<DataType>>
    {
        self.send(vec![Record::DeleteRequest(key.into())].into())
    }

    /// Perform a transactional delete from the base node this Mutator was generated for.
    pub fn transactional_delete<I>(&self, key: I, t: checktable::Token) -> Result<i64, ()>
        where I: Into<Vec<DataType>>
    {
        self.tx_send(vec![Record::DeleteRequest(key.into())].into(), t)
    }

    /// Perform a non-transactional update (delete followed by put) to the base node this Mutator
    /// was generated for.
    pub fn update<V>(&self, u: V)
        where V: Into<Vec<DataType>>
    {
        assert!(!self.primary_key.is_empty(),
                "update operations can only be applied to base nodes with key columns");

        let u = u.into();
        self.send(vec![Record::DeleteRequest(self.primary_key
                                                 .iter()
                                                 .map(|&col| &u[col])
                                                 .cloned()
                                                 .collect()),
                       u.into()]
                          .into())
    }

    /// Perform a transactional update (delete followed by put) to the base node this Mutator was
    /// generated for.
    pub fn transactional_update<V>(&self, u: V, t: checktable::Token) -> Result<i64, ()>
        where V: Into<Vec<DataType>>
    {
        assert!(!self.primary_key.is_empty(),
                "update operations can only be applied to base nodes with key columns");

        let u = u.into();
        let m = vec![Record::DeleteRequest(self.primary_key
                                               .iter()
                                               .map(|&col| &u[col])
                                               .cloned()
                                               .collect()),
                     u.into()]
                .into();
        self.tx_send(m, t)
    }

    /// Attach a tracer to all packets sent until `stop_tracing` is called. The tracer will cause
    /// events to be sent to the returned Receiver indicating the progress of the packet through the
    /// graph.
    pub fn start_tracing(&mut self) -> mpsc::Receiver<(time::Instant, PacketEvent)> {
        let (tx, rx) = mpsc::channel();
        self.tracer = Some(tx);
        rx
    }

    /// Stop attaching the tracer to packets sent.
    pub fn stop_tracing(&mut self) {
        self.tracer = None;
    }
}
