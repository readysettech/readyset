use basics::*;
use channel::{tcp, DomainConnectionBuilder, TcpSender};
use debug::trace::Tracer;
use nom_sql::CreateTableStatement;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use vec_map::VecMap;
use {ExclusiveConnection, SharedConnection, TransportError};

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct Input {
    pub link: Link,
    pub data: Vec<TableOperation>,
    pub tracer: Tracer,
}

/// A failed Table operation.
#[derive(Debug, Fail)]
pub enum TableError {
    /// The wrong number of columns was given when inserting a row.
    #[fail(display = "wrong number of columns specified: expected {}, got {}", _0, _1)]
    WrongColumnCount(usize, usize),
    /// The wrong number of key columns was given when modifying a row.
    #[fail(display = "wrong number of key columns used: expected {}, got {}", _0, _1)]
    WrongKeyColumnCount(usize, usize),
    /// The underlying connection to Soup produced an error.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] TransportError),
}

impl From<TransportError> for TableError {
    fn from(e: TransportError) -> Self {
        TableError::TransportError(e)
    }
}

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct TableBuilder {
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

impl TableBuilder {
    /// Set the local port to bind to when making the shared connection.
    pub(crate) fn with_local_port(mut self, port: u16) -> TableBuilder {
        self.local_port = Some(port);
        self
    }

    pub(crate) fn build(
        self,
        rpcs: &mut HashMap<Vec<SocketAddr>, TableRpc>,
    ) -> io::Result<Table<SharedConnection>> {
        use std::collections::hash_map::Entry;

        let dih = match rpcs.entry(self.txs.clone()) {
            Entry::Occupied(e) => Rc::clone(e.get()),
            Entry::Vacant(h) => {
                let c = DomainInputHandle::new_on(self.local_port, h.key())?;
                let c = Rc::new(RefCell::new(c));
                h.insert(Rc::clone(&c));
                c
            }
        };

        Ok(Table {
            domain_input_handle: dih,
            shard_addrs: self.txs,
            addr: self.addr,
            key: self.key,
            key_is_primary: self.key_is_primary,
            dropped: self.dropped,
            tracer: None,
            table_name: self.table_name,
            columns: self.columns,
            schema: self.schema,
            exclusivity: SharedConnection,
        })
    }
}

/// A `Table` is used to perform writes, deletes, and other operations to data in base tables.
///
/// If you create multiple `Table` handles from a single `ControllerHandle`, they may share
/// connections to the Soup workers. For this reason, `Table` is *not* `Send` or `Sync`. To get a
/// handle that can be sent to a different thread (i.e., one with its own dedicated connections),
/// call `Table::into_exclusive`.
pub struct Table<E = SharedConnection> {
    domain_input_handle: TableRpc,
    shard_addrs: Vec<SocketAddr>,
    addr: LocalNodeIndex,
    key_is_primary: bool,
    key: Vec<usize>,
    dropped: VecMap<DataType>,
    tracer: Tracer,
    table_name: String,
    columns: Vec<String>,
    schema: Option<CreateTableStatement>,

    #[allow(dead_code)]
    exclusivity: E,
}

impl Clone for Table<SharedConnection> {
    fn clone(&self) -> Self {
        Table {
            domain_input_handle: self.domain_input_handle.clone(),
            shard_addrs: self.shard_addrs.clone(),
            addr: self.addr,
            key_is_primary: self.key_is_primary,
            key: self.key.clone(),
            dropped: self.dropped.clone(),
            tracer: None,
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            exclusivity: SharedConnection,
        }
    }
}

unsafe impl Send for Table<ExclusiveConnection> {}

impl Table<SharedConnection> {
    /// Produce a `Table` with dedicated Soup connections so it can be safely sent across threads.
    pub fn into_exclusive(self) -> io::Result<Table<ExclusiveConnection>> {
        let c = DomainInputHandle::new(&self.shard_addrs[..])?;
        let c = Rc::new(RefCell::new(c));

        Ok(Table {
            domain_input_handle: c,
            shard_addrs: self.shard_addrs,
            addr: self.addr,
            key_is_primary: self.key_is_primary,
            key: self.key.clone(),
            dropped: self.dropped.clone(),
            tracer: None,
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            schema: self.schema.clone(),
            exclusivity: ExclusiveConnection,
        })
    }
}

impl<E> Table<E> {
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
    pub fn schema(&self) -> Option<&CreateTableStatement> {
        self.schema.as_ref()
    }

    /// Get the local address this `Table` is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.domain_input_handle.borrow().local_addr()
    }

    fn inject_dropped_cols(&self, rs: &mut [TableOperation]) {
        let ndropped = self.dropped.len();
        if ndropped != 0 {
            // inject defaults for dropped columns
            let dropped = self.dropped.iter().rev();
            for r in rs {
                use std::mem;

                // get a handle to the underlying data vector
                let r = match *r {
                    TableOperation::Insert(ref mut row)
                    | TableOperation::InsertOrUpdate { ref mut row, .. } => row,
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

    fn prep_records(&self, tracer: Tracer, mut ops: Vec<TableOperation>) -> Input {
        self.inject_dropped_cols(&mut ops);
        Input {
            link: Link::new(self.addr, self.addr),
            data: ops,
            tracer: tracer,
        }
    }

    fn send(&mut self, ops: Vec<TableOperation>) -> Result<(), TransportError> {
        let tracer = self.tracer.take();
        let m = self.prep_records(tracer, ops);
        self.domain_input_handle
            .borrow_mut()
            .base_send(m, &self.key[..])
    }

    /// Perform multiple operations on this base table in one batch.
    pub fn batch_insert<I, V>(&mut self, i: I) -> Result<(), TableError>
    where
        I: IntoIterator<Item = V>,
        V: Into<TableOperation>,
    {
        let mut dih = self.domain_input_handle.borrow_mut();
        let mut batch_putter = dih.sender();

        for row in i {
            let data = vec![row.into()];

            if let Some(cols) = data[0].row() {
                if cols.len() != self.columns.len() {
                    return Err(TableError::WrongColumnCount(self.columns.len(), cols.len()));
                }
            }

            let tracer = self.tracer.clone();
            let m = self.prep_records(tracer, data);
            batch_putter.enqueue(m, &self.key[..])?;
        }

        self.tracer.take();
        Ok(batch_putter.wait()?)
    }

    /// Insert a single row of data into this base table.
    pub fn insert<V>(&mut self, u: V) -> Result<(), TableError>
    where
        V: Into<Vec<DataType>>,
    {
        let data = vec![TableOperation::Insert(u.into())];
        if data[0].row().unwrap().len() != self.columns.len() {
            return Err(TableError::WrongColumnCount(
                self.columns.len(),
                data[0].row().unwrap().len(),
            ));
        }

        Ok(self.send(data)?)
    }

    /// Insert multiple rows of data into this base table.
    pub fn insert_all<I, V>(&mut self, i: I) -> Result<(), TableError>
    where
        I: IntoIterator<Item = V>,
        V: Into<Vec<DataType>>,
    {
        i.into_iter()
            .map(|r| {
                let row = r.into();
                if row.len() != self.columns.len() {
                    return Err(TableError::WrongColumnCount(self.columns.len(), row.len()));
                }
                Ok(TableOperation::Insert(row))
            })
            .collect::<Result<Vec<_>, _>>()
            .and_then(|data| Ok(self.send(data)?))
            .map(|_| ())
    }

    /// Delete the row with the given key from this base table.
    pub fn delete<I>(&mut self, key: I) -> Result<(), TableError>
    where
        I: Into<Vec<DataType>>,
    {
        Ok(self.send(vec![TableOperation::Delete { key: key.into() }].into())?)
    }

    /// Update the row with the given key in this base table.
    ///
    /// `u` is a set of column-modification pairs, where for each pair `(i, m)`, the modification
    /// `m` will be applied to column `i` of the record with key `key`.
    pub fn update<V>(&mut self, key: Vec<DataType>, u: V) -> Result<(), TableError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if key.len() != self.key.len() {
            return Err(TableError::WrongKeyColumnCount(self.key.len(), key.len()));
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in u {
            if coli >= self.columns.len() {
                return Err(TableError::WrongColumnCount(self.columns.len(), coli + 1));
            }
            set[coli] = m;
        }
        Ok(self.send(vec![TableOperation::Update { key, set }].into())?)
    }

    /// Perform a insert-or-update on this base table.
    ///
    /// If a row already exists for the key in `insert`, the existing row will instead be updated
    /// with the modifications in `u` (as documented in `Table::update`).
    pub fn insert_or_update<V>(
        &mut self,
        insert: Vec<DataType>,
        update: V,
    ) -> Result<(), TableError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if insert.len() != self.columns.len() {
            return Err(TableError::WrongColumnCount(
                self.columns.len(),
                insert.len(),
            ));
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in update {
            if coli >= self.columns.len() {
                return Err(TableError::WrongColumnCount(self.columns.len(), coli + 1));
            }
            set[coli] = m;
        }

        Ok(self.send(
            vec![TableOperation::InsertOrUpdate {
                row: insert,
                update: set,
            }].into(),
        )?)
    }

    /// Trace the next modification to this base table.
    ///
    /// When an input is traced, events are triggered as it flows through the dataflow, and are
    /// communicated back to the controller. Currently, you can only receive trace events if you
    /// build a local controller, specifically by calling `create_debug_channel` before adding any
    /// operators to the dataflow.
    ///
    /// Traced events are sent on the debug channel, and are tagged with the given `tag`.
    pub fn trace_next(&mut self, tag: u64) {
        self.tracer = Some((tag, None));
    }
}

pub(crate) struct DomainInputHandle {
    txs: Vec<TcpSender<Input>>,
}

pub(crate) type TableRpc = Rc<RefCell<DomainInputHandle>>;

impl DomainInputHandle {
    pub(crate) fn new_on(mut local_port: Option<u16>, txs: &[SocketAddr]) -> io::Result<Self> {
        let txs: io::Result<Vec<_>> = txs
            .into_iter()
            .map(|addr| {
                let c = DomainConnectionBuilder::for_base(*addr)
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

    pub(crate) fn base_send(&mut self, i: Input, key: &[usize]) -> Result<(), TransportError> {
        let mut s = BatchSendHandle::new(self);
        s.enqueue(i, key)?;
        s.wait().map_err(|_| {
            tcp::SendError::IoError(io::Error::new(io::ErrorKind::Other, "write failed")).into()
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

    pub(crate) fn enqueue(&mut self, mut i: Input, key: &[usize]) -> Result<(), TransportError> {
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
                        TableOperation::Insert(ref r) => &r[key_col],
                        TableOperation::Delete { ref key } => &key[0],
                        TableOperation::Update { ref key, .. } => &key[0],
                        TableOperation::InsertOrUpdate { ref row, .. } => &row[key_col],
                    };
                    shard_by(key, self.dih.txs.len())
                };
                shard_writes[shard].push(r);
            }

            for (s, rs) in shard_writes.drain(..).enumerate() {
                if !rs.is_empty() {
                    self.dih.txs[s].send(Input {
                        link: i.link,
                        tracer: i.tracer.clone(),
                        data: rs,
                    })?;
                    self.sent[s] += 1;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn wait(self) -> Result<(), TransportError> {
        for (shard, n) in self.sent.into_iter().enumerate() {
            for _ in 0..n {
                use bincode;
                bincode::deserialize_from(&mut (&mut self.dih.txs[shard]).reader())?;
            }
        }

        Ok(())
    }
}
