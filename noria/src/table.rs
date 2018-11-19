use async_bincode::{AsyncBincodeStream, AsyncDestination};
use crate::data::*;
use crate::debug::trace::Tracer;
use crate::internal::*;
use crate::LocalOrNot;
use crate::{Tagged, Tagger};
use futures::stream::futures_unordered::FuturesUnordered;
use nom_sql::CreateTableStatement;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::prelude::*;
use tokio_tower::multiplex;
use tokio_tower::NewTransport;
use tower_balance::{choose, Pool};
use tower_buffer::Buffer;
use tower_service::Service;
use vec_map::VecMap;

type Transport = AsyncBincodeStream<
    tokio::net::tcp::TcpStream,
    Tagged<()>,
    Tagged<LocalOrNot<Input>>,
    AsyncDestination,
>;

#[derive(Debug)]
#[doc(hidden)]
// only pub because we use it to figure out the error type for TableError
pub struct TableEndpoint(SocketAddr);

impl NewTransport<Tagged<LocalOrNot<Input>>> for TableEndpoint {
    type InitError = tokio::io::Error;
    type Transport = multiplex::MultiplexTransport<Transport, Tagger>;
    type TransportFut = Box<Future<Item = Self::Transport, Error = Self::InitError> + Send>;

    fn new_transport(&self) -> Self::TransportFut {
        Box::new(
            tokio::net::TcpStream::connect(&self.0)
                .map(AsyncBincodeStream::from)
                .map(AsyncBincodeStream::for_async)
                .map(|t| multiplex::MultiplexTransport::new(t, Tagger::default())),
        )
    }
}

pub(crate) type TableRpc = Buffer<
    Pool<
        choose::RoundRobin,
        multiplex::client::Maker<TableEndpoint, Tagged<LocalOrNot<Input>>>,
        (),
        Tagged<LocalOrNot<Input>>,
    >,
    Tagged<LocalOrNot<Input>>,
>;

type E = tower_buffer::Error<
    tower_balance::Error<
        tower_buffer::Error<
            tokio_tower::multiplex::client::Error<
                tokio_tower::multiplex::MultiplexTransport<Transport, Tagger>,
            >,
        >,
        tokio_tower::multiplex::client::SpawnError<std::io::Error>,
    >,
>;

/// A failed Table operation.
#[derive(Debug, Fail)]
pub enum TableError {
    /// The wrong number of columns was given when inserting a row.
    #[fail(
        display = "wrong number of columns specified: expected {}, got {}",
        _0, _1
    )]
    WrongColumnCount(usize, usize),

    /// The wrong number of key columns was given when modifying a row.
    #[fail(
        display = "wrong number of key columns used: expected {}, got {}",
        _0, _1
    )]
    WrongKeyColumnCount(usize, usize),

    /// The underlying connection to Noria produced an error.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] E),
}

impl From<E> for TableError {
    fn from(e: E) -> Self {
        TableError::TransportError(e)
    }
}

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct Input {
    pub dst: LocalNodeIndex,
    pub data: Vec<TableOperation>,
    pub tracer: Tracer,
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
}

impl TableBuilder {
    pub(crate) fn build(
        self,
        rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    ) -> impl Future<Item = Table, Error = io::Error> {
        future::join_all(
            self.txs
                .clone()
                .into_iter()
                .enumerate()
                .map(move |(shardi, addr)| {
                    use std::collections::hash_map::Entry;

                    // one entry per shard so that we can send sharded requests in parallel even if
                    // they happen to be targeting the same machine.
                    let mut rpcs = rpcs.lock().unwrap();
                    match rpcs.entry((addr, shardi)) {
                        Entry::Occupied(e) => Ok((addr, e.get().clone())),
                        Entry::Vacant(h) => {
                            // TODO: maybe always use the same local port?
                            let c = Buffer::new(
                                Pool::new(
                                    multiplex::client::Maker::new(TableEndpoint(addr)),
                                    (),
                                    choose::RoundRobin::default(),
                                ),
                                0,
                                &tokio::executor::DefaultExecutor::current(),
                            )
                            .unwrap_or_else(|_| panic!("no active tokio runtime"));
                            h.insert(c.clone());
                            Ok((addr, c))
                        }
                    }
                }),
        )
        .map(move |shards| {
            let (addrs, conns) = shards.into_iter().unzip();
            Table {
                node: self.addr,
                key: self.key,
                key_is_primary: self.key_is_primary,
                columns: self.columns,
                dropped: self.dropped,
                tracer: None,
                table_name: self.table_name,
                schema: self.schema,
                dst_is_local: false,

                shard_addrs: addrs,
                shards: conns,
            }
        })
    }
}

/// A `Table` is used to perform writes, deletes, and other operations to data in base tables.
///
/// If you create multiple `Table` handles from a single `ControllerHandle`, they may share
/// connections to the Soup workers. For this reason, `Table` is *not* `Send` or `Sync`. To get a
/// handle that can be sent to a different thread (i.e., one with its own dedicated connections),
/// call `Table::into_exclusive`.
#[derive(Clone)]
pub struct Table {
    node: LocalNodeIndex,
    key_is_primary: bool,
    key: Vec<usize>,
    columns: Vec<String>,
    dropped: VecMap<DataType>,
    tracer: Tracer,
    table_name: String,
    schema: Option<CreateTableStatement>,
    dst_is_local: bool,

    shards: Vec<TableRpc>,
    shard_addrs: Vec<SocketAddr>,
}

impl Service<Input> for Table {
    type Error = TableError;
    type Response = <TableRpc as Service<Tagged<LocalOrNot<Input>>>>::Response;
    type Future = Box<Future<Item = Tagged<()>, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        for s in &mut self.shards {
            try_ready!(s.poll_ready().map_err(TableError::from));
        }
        Ok(Async::Ready(()))
    }

    fn call(&mut self, mut i: Input) -> Self::Future {
        i.tracer = self.tracer.take();

        // TODO: check each row's .len() against self.columns.len() -> WrongColumnCount

        if self.shards.len() == 1 {
            Box::new(
                self.shards[0]
                    .call(
                        if self.dst_is_local {
                            unsafe { LocalOrNot::for_local_transfer(i) }
                        } else {
                            LocalOrNot::new(i)
                        }
                        .into(),
                    )
                    .map_err(TableError::from),
            )
        } else {
            if self.key.is_empty() {
                unreachable!("sharded base without a key?");
            }
            if self.key.len() != 1 {
                // base sharded by complex key
                unimplemented!();
            }
            let key_col = self.key[0];

            let mut shard_writes = vec![Vec::new(); self.shards.len()];
            for r in i.data.drain(..) {
                let shard = {
                    let key = match r {
                        TableOperation::Insert(ref r) => &r[key_col],
                        TableOperation::Delete { ref key } => &key[0],
                        TableOperation::Update { ref key, .. } => &key[0],
                        TableOperation::InsertOrUpdate { ref row, .. } => &row[key_col],
                    };
                    crate::shard_by(key, self.shards.len())
                };
                shard_writes[shard].push(r);
            }

            let mut wait_for = FuturesUnordered::new();
            for (s, rs) in shard_writes.drain(..).enumerate() {
                if !rs.is_empty() {
                    let p = if self.dst_is_local {
                        unsafe {
                            LocalOrNot::for_local_transfer(Input {
                                dst: i.dst,
                                tracer: i.tracer.clone(),
                                data: rs,
                            })
                        }
                    } else {
                        LocalOrNot::new(Input {
                            dst: i.dst,
                            tracer: i.tracer.clone(),
                            data: rs,
                        })
                    };

                    wait_for.push(self.shards[s].call(p.into()));
                }
            }

            Box::new(
                wait_for
                    .fold((), |_, _| Ok(()))
                    .map_err(TableError::from)
                    .map(|_| Tagged::from(())),
            )
        }
    }
}

impl Service<TableOperation> for Table {
    type Error = TableError;
    type Response = <Table as Service<Input>>::Response;
    type Future = <Table as Service<Input>>::Future;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        <Table as Service<Input>>::poll_ready(self)
    }

    fn call(&mut self, op: TableOperation) -> Self::Future {
        let i = self.prep_records(vec![op.into()]);
        <Table as Service<Input>>::call(self, i)
    }
}

impl Service<Vec<TableOperation>> for Table {
    type Error = TableError;
    type Response = <Table as Service<Input>>::Response;
    type Future = <Table as Service<Input>>::Future;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        <Table as Service<Input>>::poll_ready(self)
    }

    fn call(&mut self, ops: Vec<TableOperation>) -> Self::Future {
        let i = self.prep_records(ops);
        <Table as Service<Input>>::call(self, i)
    }
}

impl Table {
    /// Get the name of this base table.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[doc(hidden)]
    pub fn i_promise_dst_is_same_process(&mut self) {
        self.dst_is_local = true;
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

    fn inject_dropped_cols(&self, r: &mut TableOperation) {
        use std::mem;
        let ndropped = self.dropped.len();
        if ndropped != 0 {
            // inject defaults for dropped columns
            let dropped = self.dropped.iter().rev();

            // get a handle to the underlying data vector
            let r = match *r {
                TableOperation::Insert(ref mut row)
                | TableOperation::InsertOrUpdate { ref mut row, .. } => row,
                _ => unimplemented!("we need to shift the update/delete cols!"),
            };

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
            for (next_insert, default) in dropped {
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
                let current = &mut r[next_insert];
                let old = mem::replace(current, default.clone());
                // the old value is uninitialized memory!
                // (remember how we called set_len above?)
                mem::forget(old);

                // here, I'll help:
                // free -= 1;
            }
        }
    }

    fn prep_records(&self, mut ops: Vec<TableOperation>) -> Input {
        for r in &mut ops {
            self.inject_dropped_cols(r);
        }

        Input {
            dst: self.node,
            data: ops,
            tracer: None,
        }
    }

    fn quick_n_dirty<Request>(&self, r: Request) -> Box<Future<Item = (), Error = TableError>>
    where
        Request: 'static,
        Self: Service<Request, Error = TableError>,
    {
        // Box is needed for https://github.com/rust-lang/rust/issues/53984
        Box::new(
            self.clone()
                .ready()
                .and_then(move |mut svc| svc.call(r))
                .map(|_| ()),
        )
    }

    /// Insert a single row of data into this base table.
    pub fn insert<V>(self, u: V) -> impl Future<Item = (), Error = TableError>
    where
        V: Into<Vec<DataType>>,
    {
        self.quick_n_dirty(TableOperation::Insert(u.into()))
    }

    /// Insert multiple rows of data into this base table.
    pub fn insert_all<I, V>(&mut self, i: I) -> impl Future<Item = (), Error = TableError>
    where
        I: IntoIterator<Item = V>,
        V: Into<Vec<DataType>>,
    {
        self.quick_n_dirty(
            i.into_iter()
                .map(|r| TableOperation::Insert(r.into()))
                .collect::<Vec<_>>(),
        )
    }

    /// Delete the row with the given key from this base table.
    pub fn delete<I>(&mut self, key: I) -> impl Future<Item = (), Error = TableError>
    where
        I: Into<Vec<DataType>>,
    {
        self.quick_n_dirty(TableOperation::Delete { key: key.into() })
    }

    /// Update the row with the given key in this base table.
    ///
    /// `u` is a set of column-modification pairs, where for each pair `(i, m)`, the modification
    /// `m` will be applied to column `i` of the record with key `key`.
    pub fn update<V>(
        &mut self,
        key: Vec<DataType>,
        u: V,
    ) -> impl Future<Item = (), Error = TableError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if key.len() != self.key.len() {
            return Box::new(future::err(
                TableError::WrongKeyColumnCount(self.key.len(), key.len()).into(),
            )) as Box<_>;
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in u {
            if coli >= self.columns.len() {
                return Box::new(future::err(
                    TableError::WrongColumnCount(self.columns.len(), coli + 1).into(),
                )) as Box<_>;
            }
            set[coli] = m;
        }

        self.quick_n_dirty(TableOperation::Update { key, set })
    }

    /// Perform a insert-or-update on this base table.
    ///
    /// If a row already exists for the key in `insert`, the existing row will instead be updated
    /// with the modifications in `u` (as documented in `Table::update`).
    pub fn insert_or_update<V>(
        &mut self,
        insert: Vec<DataType>,
        update: V,
    ) -> Box<Future<Item = (), Error = <Self as Service<TableOperation>>::Error>>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if insert.len() != self.columns.len() {
            return Box::new(future::err(
                TableError::WrongColumnCount(self.columns.len(), insert.len()).into(),
            ));
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in update {
            if coli >= self.columns.len() {
                return Box::new(future::err(
                    TableError::WrongColumnCount(self.columns.len(), coli + 1).into(),
                ));
            }
            set[coli] = m;
        }

        self.quick_n_dirty(TableOperation::InsertOrUpdate {
            row: insert,
            update: set,
        })
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
