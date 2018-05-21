use basics::*;
use channel::{tcp, DomainConnectionBuilder, TcpSender};
use nom_sql::CreateTableStatement;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use vec_map::VecMap;
use {ExclusiveConnection, SharedConnection};

/// A failed Mutator operation.
#[derive(Debug)]
pub enum MutatorError {
    /// Incorrect number of columns specified for operations: (expected, got).
    WrongColumnCount(usize, usize),
    /// Incorrect number of key columns specified for operations: (expected, got).
    WrongKeyColumnCount(usize, usize),
}

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct MutatorBuilder {
    pub txs: Vec<SocketAddr>,
    pub addr: LocalNodeIndex,
    pub key_is_primary: bool,
    pub key: Vec<usize>,
    pub dropped: VecMap<DataType>,

    pub table_name: String,
    pub columns: Vec<String>,
    pub schema: Option<CreateTableStatement>,

    pub local_port: Option<u16>,
}

impl MutatorBuilder {
    /// Set the local port to bind to when making the shared connection.
    pub(crate) fn with_local_port(mut self, port: u16) -> MutatorBuilder {
        self.local_port = Some(port);
        self
    }

    pub(crate) fn build(
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
            dropped: self.dropped,
            //tracer: None,
            table_name: self.table_name,
            columns: self.columns,
            schema: self.schema,
            exclusivity: SharedConnection,
        }
    }
}

/// A `Mutator` is used to perform writes, deletes, and other operations to data in base tables.
///
/// If you create multiple `Mutator` handles from a single `ControllerHandle`, they may share
/// connections to the Soup workers. For this reason, `Mutator` is *not* `Send` or `Sync`. To get a
/// handle that can be sent to a different thread (i.e., one with its own dedicated connections),
/// call `Mutator::into_exclusive`.
pub struct Mutator<E = SharedConnection> {
    domain_input_handle: MutatorRpc,
    shard_addrs: Vec<SocketAddr>,
    addr: LocalNodeIndex,
    key_is_primary: bool,
    key: Vec<usize>,
    dropped: VecMap<DataType>,
    //tracer: Tracer,
    table_name: String,
    columns: Vec<String>,
    schema: Option<CreateTableStatement>,

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
            dropped: self.dropped.clone(),
            //tracer: self.tracer.clone(),
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            exclusivity: SharedConnection,
        }
    }
}

unsafe impl Send for Mutator<ExclusiveConnection> {}

impl Mutator<SharedConnection> {
    /// Produce a `Mutator` with dedicated Soup connections so it can be safely sent across threads.
    pub fn into_exclusive(self) -> Mutator<ExclusiveConnection> {
        let c = DomainInputHandle::new(&self.shard_addrs[..]).unwrap();
        let c = Rc::new(RefCell::new(c));

        Mutator {
            domain_input_handle: c,
            shard_addrs: self.shard_addrs,
            addr: self.addr,
            key_is_primary: self.key_is_primary,
            key: self.key.clone(),
            dropped: self.dropped.clone(),
            //tracer: self.tracer.clone(),
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            exclusivity: ExclusiveConnection,
        }
    }
}

impl<E> Mutator<E> {
    /// Get the name of this base table.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the list of columns in this base table.
    ///
    /// Note that this will *not* be updated if the underlying recipe changes and adds or removes
    /// columns!
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the schema that was used to create this base table.
    ///
    /// Note that this will *not* be updated if the underlying recipe changes and adds or removes
    /// columns!
    pub fn schema(&self) -> &CreateTableStatement {
        self.schema.as_ref().unwrap()
    }

    /// Get the local address this `Mutator` is bound to.
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

    /// Perform multiple operations on this base table in one batch.
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

        batch_putter.wait().map(|_| ()).map_err(|_| unreachable!())
    }

    /// Insert a single row of data into this base table.
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

    /// Insert multiple rows of data into this base table.
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

    /// Delete the row with the given key from this base table.
    pub fn delete<I>(&mut self, key: I) -> Result<(), MutatorError>
    where
        I: Into<Vec<DataType>>,
    {
        Ok(self.send(vec![BaseOperation::Delete { key: key.into() }].into()))
    }

    /// Update the row with the given key in this base table.
    ///
    /// `u` is a set of column-modification pairs, where for each pair `(i, m)`, the modification
    /// `m` will be applied to column `i` of the record with key `key`.
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

    /// Perform a insert-or-update on this base table.
    ///
    /// If a row already exists for the key in `insert`, the existing row will instead be updated
    /// with the modifications in `u` (as documented in `Mutator::update`).
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

    /*
    /// Trace subsequent packets by sending events on the global debug channel until `stop_tracing`
    /// is called. Any such events will be tagged with `tag`.
    pub fn start_tracing(&mut self, tag: u64) {
        self.tracer = Some((tag, None));
    }

    /// Stop attaching the tracer to packets sent.
    pub fn stop_tracing(&mut self) {
        self.tracer = None;
    }
    */
}

pub(crate) struct DomainInputHandle {
    txs: Vec<TcpSender<Input>>,
}

pub(crate) type MutatorRpc = Rc<RefCell<DomainInputHandle>>;

impl DomainInputHandle {
    pub(crate) fn new_on(mut local_port: Option<u16>, txs: &[SocketAddr]) -> io::Result<Self> {
        let txs: io::Result<Vec<_>> = txs
            .into_iter()
            .map(|addr| {
                let c = DomainConnectionBuilder::for_mutator(*addr)
                    .maybe_on_port(local_port)
                    .build()?;
                if local_port.is_none() {
                    local_port = Some(c.local_addr()?.port());
                }
                Ok(c)
            })
            .collect();

        Ok(Self { txs: txs? })
    }

    pub(crate) fn new(txs: &[SocketAddr]) -> Result<Self, io::Error> {
        Self::new_on(None, txs)
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.txs[0].local_addr()
    }

    pub(crate) fn sender(&mut self) -> BatchSendHandle {
        BatchSendHandle::new(self)
    }

    pub(crate) fn base_send(&mut self, i: Input, key: &[usize]) -> Result<i64, tcp::SendError> {
        let mut s = BatchSendHandle::new(self);
        s.enqueue(i, key)?;
        s.wait().map_err(|_| {
            tcp::SendError::IoError(io::Error::new(io::ErrorKind::Other, "write failed"))
        })
    }
}

pub(crate) struct BatchSendHandle<'a> {
    dih: &'a mut DomainInputHandle,
    sent: Vec<usize>,
}

impl<'a> BatchSendHandle<'a> {
    pub(crate) fn new(dih: &'a mut DomainInputHandle) -> Self {
        let sent = vec![0; dih.txs.len()];
        Self { dih, sent }
    }

    pub(crate) fn enqueue(&mut self, mut i: Input, key: &[usize]) -> Result<(), tcp::SendError> {
        if self.dih.txs.len() == 1 {
            self.dih.txs[0].send(i)?;
            self.sent[0] += 1;
        } else {
            if key.is_empty() {
                unreachable!("sharded base without a key?");
            }
            if key.len() != 1 {
                // base sharded by complex key
                unimplemented!();
            }
            let key_col = key[0];

            let mut shard_writes = vec![Vec::new(); self.dih.txs.len()];
            for r in i.data.drain(..) {
                let shard = {
                    let key = match r {
                        BaseOperation::Insert(ref r) => &r[key_col],
                        BaseOperation::Delete { ref key } => &key[0],
                        BaseOperation::Update { ref key, .. } => &key[0],
                        BaseOperation::InsertOrUpdate { ref row, .. } => &row[key_col],
                    };
                    shard_by(key, self.dih.txs.len())
                };
                shard_writes[shard].push(r);
            }

            for (s, rs) in shard_writes.drain(..).enumerate() {
                if !rs.is_empty() {
                    self.dih.txs[s].send(Input {
                        link: i.link,
                        data: rs,
                    })?;
                    self.sent[s] += 1;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn wait(self) -> Result<i64, ()> {
        let mut id = Ok(0);
        for (shard, n) in self.sent.into_iter().enumerate() {
            for _ in 0..n {
                use bincode;
                let res: Result<Result<i64, ()>, _>;
                res = bincode::deserialize_from(&mut (&mut self.dih.txs[shard]).reader());
                id = res.unwrap();
            }
        }

        // XXX: this just returns the last id :/
        id
    }
}
