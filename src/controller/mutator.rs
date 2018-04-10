use controller::domain_handle::DomainInputHandle;
use controller::{ExclusiveConnection, SharedConnection};
use dataflow::checktable;
use dataflow::prelude::*;

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use vec_map::VecMap;

/// Indicates why a Mutator operation failed.
#[derive(Serialize, Deserialize, Debug)]
pub enum MutatorError {
    /// Incorrect number of columns specified for operations: (expected, got).
    WrongColumnCount(usize, usize),
    /// Transaction was unable to complete due to a conflict.
    TransactionFailed,
}

/// Serializable struct that Mutators can be constructed from.
#[derive(Clone, Serialize, Deserialize)]
pub struct MutatorBuilder {
    pub(crate) txs: Vec<SocketAddr>,
    pub(crate) addr: LocalNodeIndex,
    pub(crate) key_is_primary: bool,
    pub(crate) key: Vec<usize>,
    pub(crate) transactional: bool,
    pub(crate) dropped: VecMap<DataType>,

    pub(crate) table_name: String,
    pub(crate) columns: Vec<String>,

    // skip so that serde will set value to default (which is false) when serializing
    #[serde(skip)]
    pub(crate) is_local: bool,
}

pub(crate) type MutatorRpc = Rc<RefCell<DomainInputHandle>>;

impl MutatorBuilder {
    /// Construct a `Mutator`.
    pub fn build(
        self,
        rpcs: &mut HashMap<Vec<SocketAddr>, MutatorRpc>,
    ) -> Mutator<SharedConnection> {
        use std::collections::hash_map::Entry;

        let dih = match rpcs.entry(self.txs.clone()) {
            Entry::Occupied(e) => Rc::clone(e.get()),
            Entry::Vacant(h) => {
                let c = DomainInputHandle::new(h.key()).unwrap();
                let c = Rc::new(RefCell::new(c));
                h.insert(Rc::clone(&c));
                c
            }
        };

        Mutator {
            domain_input_handle: dih,
            shard_addrs: self.txs,
            addr: self.addr,
            key: self.key,
            key_is_primary: self.key_is_primary,
            transactional: self.transactional,
            dropped: self.dropped,
            tracer: None,
            table_name: self.table_name,
            columns: self.columns,
            is_local: self.is_local,
            exclusivity: SharedConnection,
        }
    }
}

/// A `Mutator` is used to perform reads and writes to base nodes.
pub struct Mutator<E = SharedConnection> {
    domain_input_handle: MutatorRpc,
    shard_addrs: Vec<SocketAddr>,
    addr: LocalNodeIndex,
    key_is_primary: bool,
    key: Vec<usize>,
    transactional: bool,
    dropped: VecMap<DataType>,
    tracer: Tracer,
    table_name: String,
    columns: Vec<String>,
    is_local: bool,

    #[allow(dead_code)]
    exclusivity: E,
}

impl Clone for Mutator<SharedConnection> {
    fn clone(&self) -> Self {
        Mutator {
            domain_input_handle: self.domain_input_handle.clone(),
            shard_addrs: self.shard_addrs.clone(),
            addr: self.addr,
            key_is_primary: self.key_is_primary,
            key: self.key.clone(),
            transactional: self.transactional,
            dropped: self.dropped.clone(),
            tracer: self.tracer.clone(),
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            is_local: self.is_local,
            exclusivity: SharedConnection,
        }
    }
}

unsafe impl Send for Mutator<ExclusiveConnection> {}

impl Mutator<SharedConnection> {
    /// Change this mutator into one with a dedicated connection the server so it can be sent
    /// across threads.
    pub fn into_exclusive(self) -> Mutator<ExclusiveConnection> {
        let c = DomainInputHandle::new(&self.shard_addrs[..]).unwrap();
        let c = Rc::new(RefCell::new(c));

        Mutator {
            domain_input_handle: c,
            shard_addrs: self.shard_addrs,
            addr: self.addr,
            key_is_primary: self.key_is_primary,
            key: self.key.clone(),
            transactional: self.transactional,
            dropped: self.dropped.clone(),
            tracer: self.tracer.clone(),
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            is_local: self.is_local,
            exclusivity: ExclusiveConnection,
        }
    }
}

impl<E> Mutator<E> {
    fn inject_dropped_cols(&self, rs: &mut Records) {
        let ndropped = self.dropped.len();
        if ndropped != 0 {
            // inject defaults for dropped columns
            let dropped = self.dropped.iter().rev();
            for r in rs.iter_mut() {
                use std::mem;

                // get a handle to the underlying data vector
                let r = match *r {
                    Record::Positive(ref mut r) | Record::Negative(ref mut r) => r,
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

    fn prep_records(&self, mut rs: Records) -> Box<Packet> {
        self.inject_dropped_cols(&mut rs);
        if self.transactional {
            box Packet::Transaction {
                link: Link::new(self.addr, self.addr),
                src: None,
                data: rs,
                state: TransactionState::WillCommit,
                tracer: self.tracer.clone(),
                senders: vec![],
            }
        } else {
            box Packet::Message {
                link: Link::new(self.addr, self.addr),
                src: None,
                data: rs,
                tracer: self.tracer.clone(),
                senders: vec![],
            }
        }
    }

    fn send(&mut self, rs: Records) {
        let m = self.prep_records(rs);
        self.domain_input_handle
            .borrow_mut()
            .base_send(m, &self.key[..], self.is_local)
            .unwrap();
    }

    fn tx_send(&mut self, mut rs: Records, t: checktable::Token) -> Result<i64, ()> {
        assert!(self.transactional);

        self.inject_dropped_cols(&mut rs);
        let m = box Packet::Transaction {
            link: Link::new(self.addr, self.addr),
            src: None,
            data: rs,
            state: TransactionState::Pending(t),
            tracer: self.tracer.clone(),
            senders: vec![],
        };

        self.domain_input_handle
            .borrow_mut()
            .base_send(m, &self.key[..], self.is_local)
            .map_err(|_| ())
    }

    /// Perform a non-transactional write to the base node this Mutator was generated for.
    pub fn batch_put<I, V>(&mut self, i: I) -> Result<(), MutatorError>
    where
        I: IntoIterator<Item = V>,
        V: Into<Vec<DataType>>,
    {
        let mut dih = self.domain_input_handle.borrow_mut();
        let mut batch_putter = dih.sender();

        for row in i {
            let data = vec![row.into()];
            if data[0].len() != self.columns.len() {
                return Err(MutatorError::WrongColumnCount(
                    self.columns.len(),
                    data[0].len(),
                ));
            }

            let m = self.prep_records(data.into());
            batch_putter
                .enqueue(m, &self.key[..], self.is_local)
                .unwrap();
        }

        batch_putter
            .wait()
            .map(|_| ())
            .map_err(|_| MutatorError::TransactionFailed)
    }

    /// Perform a non-transactional write to the base node this Mutator was generated for.
    pub fn put<V>(&mut self, u: V) -> Result<(), MutatorError>
    where
        V: Into<Vec<DataType>>,
    {
        let data = vec![u.into()];
        if data[0].len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(
                self.columns.len(),
                data[0].len(),
            ));
        }

        Ok(self.send(data.into()))
    }

    /// Perform some non-transactional writes to the base node this Mutator was generated for.
    pub fn multi_put<V>(&mut self, u: V) -> Result<(), MutatorError>
    where
        V: Into<Vec<Vec<DataType>>>,
    {
        let data = u.into();
        if data[0].len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(
                self.columns.len(),
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
        if data[0].len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(
                self.columns.len(),
                data[0].len(),
            ));
        }

        self.tx_send(data.into(), t)
            .map_err(|()| MutatorError::TransactionFailed)
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
            .map_err(|()| MutatorError::TransactionFailed)
    }

    /// Perform a non-transactional update (delete followed by put) to the base node this Mutator
    /// was generated for.
    #[allow(unreachable_code)]
    pub fn update<V>(&mut self, _u: V) -> Result<(), MutatorError>
    where
        V: Into<Vec<DataType>>,
    {
        // See 869c59974349bbc6c0cab314285c35376906cba4.
        panic!("Not currently working correctly due to primary key uniqueness constraint!");

        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        let u = _u.into();
        if u.len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(self.columns.len(), u.len()));
        }

        let pkey = self.key.iter().map(|&col| &u[col]).cloned().collect();
        Ok(self.send(vec![Record::DeleteRequest(pkey), u.into()].into()))
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
        if u.len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(self.columns.len(), u.len()));
        }

        let m = vec![
            Record::DeleteRequest(self.key.iter().map(|&col| &u[col]).cloned().collect()),
            u.into(),
        ].into();
        self.tx_send(m, t)
            .map_err(|()| MutatorError::TransactionFailed)
    }

    /// Trace subsequent packets by sending events on the global debug channel until `stop_tracing`
    /// is called. Any such events will be tagged with `tag`.
    pub fn start_tracing(&mut self, tag: u64) {
        self.tracer = Some((tag, None));
    }

    /// Stop attaching the tracer to packets sent.
    pub fn stop_tracing(&mut self) {
        self.tracer = None;
    }

    /// Get the name of the base table that this mutator writes to.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the name of the columns in the base table this mutator is for.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}
