use controller::domain_handle::DomainInputHandle;
use controller::{ExclusiveConnection, SharedConnection};
use dataflow::checktable;
use dataflow::prelude::*;

use nom_sql::CreateTableStatement;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use vec_map::VecMap;

/// Indicates why a Mutator operation failed.
#[derive(Serialize, Deserialize, Debug)]
pub enum MutatorError {
    /// Incorrect number of columns specified for operations: (expected, got).
    WrongColumnCount(usize, usize),
    /// Incorrect number of key columns specified for operations: (expected, got).
    WrongKeyColumnCount(usize, usize),
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
    pub(crate) schema: Option<CreateTableStatement>,

    pub(crate) local_port: Option<u16>,

    // skip so that serde will set value to default (which is false) when serializing
    #[serde(skip)]
    pub(crate) is_local: bool,
}

pub(crate) type MutatorRpc = Rc<RefCell<DomainInputHandle>>;

impl MutatorBuilder {
    /// Set the local port to bind to when making the shared connection.
    pub(crate) fn with_local_port(mut self, port: u16) -> MutatorBuilder {
        self.local_port = Some(port);
        self
    }

    /// Construct a `Mutator`.
    pub fn build(
        self,
        rpcs: &mut HashMap<Vec<SocketAddr>, MutatorRpc>,
    ) -> Mutator<SharedConnection> {
        use std::collections::hash_map::Entry;

        let dih = match rpcs.entry(self.txs.clone()) {
            Entry::Occupied(e) => Rc::clone(e.get()),
            Entry::Vacant(h) => {
                let c = DomainInputHandle::new_on(self.local_port, h.key()).unwrap();
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
            schema: self.schema,
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
    schema: Option<CreateTableStatement>,
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
            schema: self.schema.clone(),
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
            schema: self.schema.clone(),
            is_local: self.is_local,
            exclusivity: ExclusiveConnection,
        }
    }
}

impl<E> Mutator<E> {
    /// Get the local address this mutator is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.domain_input_handle.borrow().local_addr()
    }

    fn inject_dropped_cols(&self, rs: &mut [BaseOperation]) {
        let ndropped = self.dropped.len();
        if ndropped != 0 {
            // inject defaults for dropped columns
            let dropped = self.dropped.iter().rev();
            for r in rs {
                use std::mem;

                // get a handle to the underlying data vector
                let r = match *r {
                    BaseOperation::Insert(ref mut row)
                    | BaseOperation::InsertOrUpdate { ref mut row, .. } => row,
                    _ => unimplemented!("we need to shift the update/delete cols!"),
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

    fn prep_records(&self, mut ops: Vec<BaseOperation>) -> Input {
        self.inject_dropped_cols(&mut ops);
        Input {
            link: Link::new(self.addr, self.addr),
            data: ops,
            //txn: TransactionState::WillCommit,
            //tracer: self.tracer.clone(),
        }
    }

    fn send(&mut self, ops: Vec<BaseOperation>) {
        let m = self.prep_records(ops);
        self.domain_input_handle
            .borrow_mut()
            .base_send(m, &self.key[..])
            .unwrap();
    }

    #[allow(unused_variables)]
    #[allow(unreachable_code)]
    fn tx_send(&mut self, mut ops: Vec<BaseOperation>, t: checktable::Token) -> Result<i64, ()> {
        assert!(self.transactional);

        self.inject_dropped_cols(&mut ops);
        let m = Input {
            link: Link::new(self.addr, self.addr),
            data: ops,
            //txn: TransactionState::Pending(t),
            //tracer: self.tracer.clone(),
        };

        unimplemented!();

        self.domain_input_handle
            .borrow_mut()
            .base_send(m, &self.key[..])
            .map_err(|_| ())
    }

    /// Perform a non-transactional write to the base node this Mutator was generated for.
    pub fn batch_put<I, V>(&mut self, i: I) -> Result<(), MutatorError>
    where
        I: IntoIterator<Item = V>,
        V: Into<BaseOperation>,
    {
        let mut dih = self.domain_input_handle.borrow_mut();
        let mut batch_putter = dih.sender();

        for row in i {
            let data = vec![row.into()];

            if let Some(cols) = data[0].row() {
                if cols.len() != self.columns.len() {
                    return Err(MutatorError::WrongColumnCount(
                        self.columns.len(),
                        cols.len(),
                    ));
                }
            }

            let m = self.prep_records(data);
            batch_putter.enqueue(m, &self.key[..]).unwrap();
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
        let data = vec![BaseOperation::Insert(u.into())];
        if data[0].row().unwrap().len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(
                self.columns.len(),
                data[0].row().unwrap().len(),
            ));
        }

        Ok(self.send(data))
    }

    /// Perform some non-transactional writes to the base node this Mutator was generated for.
    pub fn multi_put<I, V>(&mut self, i: I) -> Result<(), MutatorError>
    where
        I: IntoIterator<Item = V>,
        V: Into<Vec<DataType>>,
    {
        i.into_iter()
            .map(|r| {
                let row = r.into();
                if row.len() != self.columns.len() {
                    return Err(MutatorError::WrongColumnCount(
                        self.columns.len(),
                        row.len(),
                    ));
                }
                Ok(BaseOperation::Insert(row))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|data| self.send(data))
    }

    /// Perform a transactional write to the base node this Mutator was generated for.
    pub fn transactional_put<V>(&mut self, u: V, t: checktable::Token) -> Result<i64, MutatorError>
    where
        V: Into<Vec<DataType>>,
    {
        let data = vec![BaseOperation::Insert(u.into())];
        if data[0].row().unwrap().len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(
                self.columns.len(),
                data[0].row().unwrap().len(),
            ));
        }

        self.tx_send(data, t)
            .map_err(|()| MutatorError::TransactionFailed)
    }

    /// Perform a non-transactional delete from the base node this Mutator was generated for.
    pub fn delete<I>(&mut self, key: I) -> Result<(), MutatorError>
    where
        I: Into<Vec<DataType>>,
    {
        Ok(self.send(vec![BaseOperation::Delete { key: key.into() }].into()))
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
        self.tx_send(vec![BaseOperation::Delete { key: key.into() }].into(), t)
            .map_err(|()| MutatorError::TransactionFailed)
    }

    /// Perform a non-transactional update to the base node this Mutator was generated for.
    pub fn update<V>(&mut self, key: Vec<DataType>, u: V) -> Result<(), MutatorError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if key.len() != self.key.len() {
            return Err(MutatorError::WrongKeyColumnCount(self.key.len(), key.len()));
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in u {
            if coli >= self.columns.len() {
                return Err(MutatorError::WrongColumnCount(self.columns.len(), coli + 1));
            }
            set[coli] = m;
        }
        Ok(self.send(vec![BaseOperation::Update { key, set }].into()))
    }

    /// Perform a non-transactional insert-or-update to the base node this Mutator was generated
    /// for. If a record already exists for the key of the given record, the existing record will
    /// instead be updated with the modifications in `u`.
    pub fn insert_or_update<V>(
        &mut self,
        insert: Vec<DataType>,
        update: V,
    ) -> Result<(), MutatorError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if insert.len() != self.columns.len() {
            return Err(MutatorError::WrongColumnCount(
                self.columns.len(),
                insert.len(),
            ));
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in update {
            if coli >= self.columns.len() {
                return Err(MutatorError::WrongColumnCount(self.columns.len(), coli + 1));
            }
            set[coli] = m;
        }
        Ok(self.send(
            vec![BaseOperation::InsertOrUpdate {
                row: insert,
                update: set,
            }].into(),
        ))
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

    /// Get the base table schema.
    pub fn schema(&self) -> &CreateTableStatement {
        self.schema.as_ref().unwrap()
    }
}
