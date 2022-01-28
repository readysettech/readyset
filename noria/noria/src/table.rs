use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};
use std::{fmt, iter};

use async_bincode::{AsyncBincodeStream, AsyncDestination};
use derive_more::TryInto;
use noria_errors::{
    internal, internal_err, rpc_err, table_err, unsupported, ReadySetError, ReadySetResult,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use core::convert::TryInto;
use futures_util::{
    future, future::TryFutureExt, ready, stream::futures_unordered::FuturesUnordered,
    stream::TryStreamExt,
};
use itertools::Either;
use nom_sql::CreateTableStatement;
use noria_data::DataType;
use petgraph::graph::NodeIndex;
use tokio::io::AsyncWriteExt;
use tokio_tower::multiplex;
use tower::balance::p2c::Balance;
use tower::buffer::Buffer;
use tower::limit::concurrency::ConcurrencyLimit;
use tower_service::Service;
use vec_map::VecMap;

use crate::channel::CONNECTION_FROM_BASE;
use crate::internal::*;
use crate::replication::ReplicationOffset;
use crate::{consistency, LocalOrNot, Tagged, Tagger};

// TODO(justin): Make write propagation sample rate configurable.
const TRACE_SAMPLE_RATE: Duration = Duration::from_secs(1);

/// A modification to make to an existing value.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Operation {
    /// Add the given value to the existing one.
    Add,
    /// Subtract the given value from the existing value.
    Sub,
}

/// A modification to make to a column in an existing row.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Modification {
    /// Set the cell to this value.
    Set(DataType),
    /// Use the given [`Operation`] to combine the existing value and this one.
    Apply(Operation, DataType),
    /// Leave the existing value as-is.
    None,
}

impl<T> From<T> for Modification
where
    T: Into<DataType>,
{
    fn from(t: T) -> Modification {
        Modification::Set(t.into())
    }
}

/// An operation to apply to a base table.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum TableOperation {
    /// Insert the contained row.
    Insert(Vec<DataType>),
    /// Delete a row with the contained key.
    DeleteByKey {
        /// The key.
        key: Vec<DataType>,
    },
    /// Delete *one* row matching the entirety of the given row
    DeleteRow {
        /// The row to delete
        row: Vec<DataType>,
    },
    /// If a row exists with the same key as the contained row, update it using `update`, otherwise
    /// insert `row`.
    InsertOrUpdate {
        /// This row will be inserted if no existing row is found.
        row: Vec<DataType>,
        /// These modifications will be applied to the columns of an existing row.
        update: Vec<Modification>,
    },
    /// Update an existing row with the given `key`.
    Update {
        /// The modifications to make to each column of the existing row.
        update: Vec<Modification>,
        /// The key used to identify the row to update.
        key: Vec<DataType>,
    },
    /// Set the replication offset for data written to this base table.
    ///
    /// Within a group of table operations, the largest replication offset will take precedence
    ///
    /// See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
    /// more information about replication offsets.
    SetReplicationOffset(ReplicationOffset),

    /// Enter or exit snapshot mode for the underlying persistent storage. In snapshot mode
    /// compactions are disabled and writes don't go into WAL first.
    SetSnapshotMode(bool),
}

impl TableOperation {
    #[doc(hidden)]
    pub fn row(&self) -> Option<&[DataType]> {
        match *self {
            TableOperation::Insert(ref r) => Some(r),
            TableOperation::InsertOrUpdate { ref row, .. } => Some(row),
            _ => None,
        }
    }

    /// Construct an iterator over the shards this TableOperation should target.
    ///
    /// ## Invariants
    /// * `key_col` must be in the rows.
    /// * the `key`s must have at least one element.
    #[inline]
    pub fn shards(&self, key_col: usize, num_shards: usize) -> impl Iterator<Item = usize> {
        #[allow(clippy::indexing_slicing)]
        let key = match self {
            TableOperation::Insert(row) => Some(&row[key_col]),
            TableOperation::DeleteByKey { key } => Some(&key[0]),
            TableOperation::DeleteRow { row } => Some(&row[key_col]),
            TableOperation::Update { key, .. } => Some(&key[0]),
            TableOperation::InsertOrUpdate { row, .. } => Some(&row[key_col]),
            TableOperation::SetReplicationOffset(_) => None,
            TableOperation::SetSnapshotMode(_) => None,
        };

        if let Some(key) = key {
            Either::Left(iter::once(crate::shard_by(key, num_shards)))
        } else {
            // updates to replication offsets should hit all shards
            Either::Right(0..num_shards)
        }
    }
}

impl From<Vec<DataType>> for TableOperation {
    fn from(other: Vec<DataType>) -> Self {
        TableOperation::Insert(other)
    }
}

type Transport = AsyncBincodeStream<
    tokio::net::TcpStream,
    Tagged<()>,
    Tagged<LocalOrNot<PacketData>>,
    AsyncDestination,
>;

#[derive(Debug)]
struct Endpoint {
    addr: SocketAddr,
    timeout: Duration,
}

type InnerService = multiplex::Client<
    multiplex::MultiplexTransport<Transport, Tagger>,
    tokio_tower::Error<
        multiplex::MultiplexTransport<Transport, Tagger>,
        Tagged<LocalOrNot<PacketData>>,
    >,
    Tagged<LocalOrNot<PacketData>>,
>;

impl Service<()> for Endpoint {
    type Response = InnerService;
    type Error = tokio::io::Error;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let f = tokio::net::TcpStream::connect(self.addr);
        let timeout = self.timeout;
        async move {
            let mut s = tokio::time::timeout(timeout, f).await??;
            s.set_nodelay(true)?;
            s.write_all(&[CONNECTION_FROM_BASE]).await?;
            s.flush().await?;
            let s = AsyncBincodeStream::from(s).for_async();
            let t = multiplex::MultiplexTransport::new(s, Tagger::default());
            Ok(multiplex::Client::with_error_handler(
                t,
                |e| error!(error = %e, "Table server went away"),
            ))
        }
    }
}

fn make_table_stream(
    addr: SocketAddr,
    timeout: Duration,
) -> impl futures_util::stream::TryStream<
    Ok = tower::discover::Change<usize, InnerService>,
    Error = tokio::io::Error,
> {
    // TODO: use whatever comes out of https://github.com/tower-rs/tower/issues/456 instead of
    // creating _all_ the connections every time.
    (0..crate::TABLE_POOL_SIZE)
        .map(|i| async move {
            let svc = Endpoint { addr, timeout }.call(()).await?;
            Ok(tower::discover::Change::Insert(i, svc))
        })
        .collect::<futures_util::stream::FuturesUnordered<_>>()
}

fn make_table_discover(addr: SocketAddr, timeout: Duration) -> Discover {
    make_table_stream(addr, timeout)
}

// Unpin + Send bounds are needed due to https://github.com/rust-lang/rust/issues/55997
type Discover = impl tower::discover::Discover<Key = usize, Service = InnerService, Error = tokio::io::Error>
    + Unpin
    + Send;

pub(crate) type TableRpc = Buffer<
    ConcurrencyLimit<Balance<Discover, Tagged<LocalOrNot<PacketData>>>>,
    Tagged<LocalOrNot<PacketData>>,
>;

/// Information used to uniquely identify: a packet, and the time a packet entered the
/// system.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PacketTrace {
    /// Time that the packet trace was initiated at. Currently used to measure the
    /// end-to-end trace duration. Comparing this value to other recorded system
    /// clock values comes with caveats. The system clock on a single machine may
    /// be adjusted, and system time values across machines is subject to
    /// synchronization issues.
    pub start: SystemTime,
}

/// Wrapper of packet payloads with their destination node.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct PacketData {
    /// The domain identifier of the destination node.
    pub dst: LocalNodeIndex,
    /// The data associated with the packet.
    pub data: PacketPayload,
    /// Optional packet trace to associate with the packet.
    pub trace: Option<PacketTrace>,
}

/// Wrapper around types that can be propagated to base tables
/// as packets.
#[derive(Clone, Serialize, Deserialize, TryInto, PartialEq)]
#[try_into(owned, ref, ref_mut)]
pub enum PacketPayload {
    /// An input update to a base table.
    Input(Vec<TableOperation>),
    /// A new timestamp to update the base table.
    Timestamp(consistency::Timestamp),
}

impl fmt::Debug for PacketData {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Input").field("dst", &self.dst).finish()
    }
}

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct TableBuilder {
    pub txs: Vec<SocketAddr>,
    pub ni: NodeIndex,
    pub addr: LocalNodeIndex,
    pub key_is_primary: bool,
    pub key: Vec<usize>,
    pub dropped: VecMap<DataType>,

    pub table_name: String,
    pub columns: Vec<String>,
    pub schema: Option<CreateTableStatement>,

    /// The amount of time before a table request RPC is terminated.
    pub table_request_timeout: Duration,
}

impl TableBuilder {
    pub(crate) fn build(self, rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>) -> Table {
        let mut addrs = Vec::with_capacity(self.txs.len());
        let mut conns = Vec::with_capacity(self.txs.len());
        for (shardi, &addr) in self.txs.iter().enumerate() {
            use std::collections::hash_map::Entry;

            addrs.push(addr);

            // one entry per shard so that we can send sharded requests in parallel even if
            // they happen to be targeting the same machine.
            #[allow(clippy::unwrap_used)]
            // This can only fail if the mutex is poisoned, in which case we want to panic
            // since there's no way to recover.
            let mut rpcs = rpcs.lock().unwrap();
            let s = match rpcs.entry((addr, shardi)) {
                Entry::Occupied(e) => e.get().clone(),
                Entry::Vacant(h) => {
                    // TODO: maybe always use the same local port?
                    let (c, w) = Buffer::pair(
                        ConcurrencyLimit::new(
                            Balance::new(make_table_discover(addr, self.table_request_timeout)),
                            crate::PENDING_LIMIT,
                        ),
                        crate::BUFFER_TO_POOL,
                    );
                    use tracing_futures::Instrument;
                    tokio::spawn(w.instrument(tracing::debug_span!(
                        "table_worker",
                        addr = %addr,
                        shard = shardi
                    )));
                    h.insert(c.clone());
                    c
                }
            };
            conns.push(s);
        }

        Table {
            ni: self.ni,
            node: self.addr,
            key: self.key,
            key_is_primary: self.key_is_primary,
            columns: self.columns,
            dropped: self.dropped,
            table_name: self.table_name,
            schema: self.schema,
            dst_is_local: false,
            shard_addrs: addrs,
            shards: conns,
            last_trace_sample: Instant::now(),
            request_timeout: self.table_request_timeout,
        }
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
    ni: NodeIndex,
    /// The LocalNodeIndex assigned to the table.
    pub node: LocalNodeIndex,
    key_is_primary: bool,
    key: Vec<usize>,
    columns: Vec<String>,
    dropped: VecMap<DataType>,
    table_name: String,
    schema: Option<CreateTableStatement>,
    dst_is_local: bool,
    shards: Vec<TableRpc>,
    shard_addrs: Vec<SocketAddr>,
    last_trace_sample: Instant,
    request_timeout: Duration,
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("ni", &self.ni)
            .field("node", &self.node)
            .field("key_is_primary", &self.key_is_primary)
            .field("key", &self.key)
            .field("columns", &self.columns)
            .field("dropped", &self.dropped)
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("dst_is_local", &self.dst_is_local)
            .field("shard_addrs", &self.shard_addrs)
            .finish()
    }
}

impl Table {
    #[allow(clippy::cognitive_complexity)]
    fn input(
        &mut self,
        mut i: PacketData,
    ) -> impl Future<Output = Result<Tagged<()>, ReadySetError>> + Send {
        let span = if crate::trace_next_op() {
            Some(tracing::trace_span!(
                "table-request",
                base = self.ni.index()
            ))
        } else {
            None
        };

        // NOTE: this is really just a try block
        let immediate_err = || {
            let ncols = self.columns.len() + self.dropped.len();
            let ops: &Vec<TableOperation> = (&i.data)
                .try_into()
                .map_err(|_| ReadySetError::WrongPacketDataType)?;
            for op in ops {
                match op {
                    TableOperation::Insert(ref row) | TableOperation::DeleteRow { ref row } => {
                        if row.len() != ncols {
                            return Err(ReadySetError::WrongColumnCount(ncols, row.len()));
                        }
                    }
                    TableOperation::DeleteByKey { ref key } => {
                        if key.len() != self.key.len() {
                            return Err(ReadySetError::WrongKeyColumnCount(
                                self.key.len(),
                                key.len(),
                            ));
                        }
                    }
                    TableOperation::InsertOrUpdate {
                        ref row,
                        ref update,
                    } => {
                        if row.len() != ncols {
                            return Err(ReadySetError::WrongColumnCount(ncols, row.len()));
                        }
                        if update.len() > self.columns.len() {
                            // NOTE: < is okay to allow dropping tailing no-ops
                            return Err(ReadySetError::WrongColumnCount(
                                self.columns.len(),
                                update.len(),
                            ));
                        }
                    }
                    TableOperation::Update {
                        ref update,
                        ref key,
                    } => {
                        if key.len() != self.key.len() {
                            return Err(ReadySetError::WrongKeyColumnCount(
                                self.key.len(),
                                key.len(),
                            ));
                        }
                        if update.len() > self.columns.len() {
                            // NOTE: < is okay to allow dropping tailing no-ops
                            return Err(ReadySetError::WrongColumnCount(
                                self.columns.len(),
                                update.len(),
                            ));
                        }
                    }
                    TableOperation::SetReplicationOffset(_) => {}
                    TableOperation::SetSnapshotMode(_) => {}
                }
            }
            Ok(())
        };

        if let Err(e) = immediate_err() {
            return future::Either::Left(future::Either::Left(async move { Err(e) }));
        }

        let nshards = self.shards.len();
        future::Either::Right(match self.shards.first_mut() {
            Some(table_rpc) if nshards == 1 => {
                let request =
                    Tagged::from(unsafe { LocalOrNot::new_for_dst(i, self.dst_is_local) });
                let _guard = span.as_ref().map(tracing::Span::enter);
                tracing::trace!("submit request");
                future::Either::Left(future::Either::Right(
                    table_rpc.call(request).map_err(rpc_err!("Table::input")),
                ))
            }
            _ => {
                let key_len = self.key.len();
                let key_col = match self.key.get(0) {
                    // If it's `None`, then it's empty.
                    None => {
                        return future::Either::Right(future::Either::Left(future::Either::Left(
                            future::Either::Left(
                                async move { internal!("sharded base without a key") },
                            ),
                        )))
                    }
                    Some(_) if key_len != 1 => {
                        return future::Either::Right(future::Either::Left(future::Either::Left(
                            future::Either::Right(async move {
                                internal!("base sharded by complex key")
                            }),
                        )))
                    }
                    Some(&k) => k,
                };

                let _guard = span.as_ref().map(tracing::Span::enter);
                tracing::trace!("shard request");
                let mut shard_writes = vec![Vec::new(); nshards];
                let ops: &mut Vec<TableOperation> = match (&mut i.data).try_into() {
                    Ok(v) => v,
                    Err(e) => {
                        return future::Either::Left(future::Either::Right(async move {
                            internal!("couldn't get table operations from packet. Error: '{}'", e)
                        }))
                    }
                };
                for r in ops.drain(..) {
                    for shard in r.shards(key_col, nshards) {
                        // The `shard` index belongs to the range `0..nshards`,
                        // so it's not out of bounds.
                        #[allow(clippy::indexing_slicing)]
                        shard_writes[shard].push(r.clone())
                    }
                }

                let wait_for = FuturesUnordered::new();
                for (s, rs) in shard_writes.drain(..).enumerate() {
                    if !rs.is_empty() {
                        let new_i = PacketData {
                            dst: i.dst,
                            data: PacketPayload::Input(rs),
                            trace: i.trace.clone(),
                        };

                        let p = unsafe { LocalOrNot::new_for_dst(new_i, self.dst_is_local) };
                        let request = Tagged::from(p);

                        // make a span per shard
                        let span = if span.is_some() {
                            Some(tracing::trace_span!("table-shard", s))
                        } else {
                            None
                        };
                        let _guard = span.as_ref().map(tracing::Span::enter);
                        tracing::trace!("submit request shard");

                        // `s` is within the range of `0..nshards`, so it is not out of bounds
                        // for the `self.shards` vector.
                        #[allow(clippy::indexing_slicing)]
                        wait_for.push(self.shards[s].call(request));
                    } else {
                        // poll_ready reserves a sender slot which we have to release
                        // we do that by dropping the old handle and replacing it with a clone
                        // https://github.com/tokio-rs/tokio/issues/898
                        // `s` is within the range of `0..nshards`, so it is not out of bounds
                        // for the `self.shards` vector.
                        // This is also inside the block so the annotation applies to both terms
                        // in the assignment.
                        #[allow(clippy::indexing_slicing)]
                        {
                            self.shards[s] = self.shards[s].clone()
                        }
                    }
                }

                future::Either::Right(
                    wait_for
                        .try_for_each(|_| async { Ok(()) })
                        .map_err(rpc_err!("Table::input"))
                        .map_ok(Tagged::from),
                )
            }
        })
    }

    /// Sends the timestamp `PacketData` to each base table shard associated with
    /// `self`.
    fn timestamp(
        &mut self,
        t: PacketData,
    ) -> impl Future<Output = Result<Tagged<()>, ReadySetError>> + Send {
        let nshards = self.shards.len();
        match self.shards.first_mut() {
            Some(table_rpc) if nshards == 1 => {
                let request =
                    Tagged::from(unsafe { LocalOrNot::new_for_dst(t, self.dst_is_local) });
                future::Either::Left(
                    table_rpc
                        .call(request)
                        .map_err(rpc_err!("Table::timestamp")),
                )
            }
            _ => {
                if self.key.is_empty() {
                    return future::Either::Right(future::Either::Left(future::Either::Left(
                        async move { internal!("sharded base without a key?") },
                    )));
                }
                if self.key.len() != 1 {
                    // base sharded by complex key
                    return future::Either::Right(future::Either::Left(future::Either::Right(
                        async move { internal!("sharded base without a key?") },
                    )));
                }

                // We create a request to each base table shard with the new timestamp.
                let wait_for = FuturesUnordered::new();
                for s in &mut self.shards {
                    let p = unsafe { LocalOrNot::new_for_dst(t.clone(), self.dst_is_local) };
                    let request = Tagged::from(p);
                    wait_for.push(s.call(request));
                }
                future::Either::Right(future::Either::Right(
                    wait_for
                        .try_for_each(|_| async { Ok(()) })
                        .map_err(rpc_err!("Table::timestamp"))
                        .map_ok(Tagged::from),
                ))
            }
        }
    }
}

/// A request to the table service.
pub enum TableRequest {
    /// A set of operations to apply on the table.
    TableOperations(Vec<TableOperation>),
    /// A timestamp to propagate along the data flow from the base table.
    Timestamp(consistency::Timestamp),
}

impl Service<TableRequest> for Table {
    type Error = ReadySetError;
    type Response = <TableRpc as Service<Tagged<LocalOrNot<PacketData>>>>::Response;

    type Future = impl Future<Output = Result<Tagged<()>, ReadySetError>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        for s in &mut self.shards {
            ready!(s.poll_ready(cx))
                .map_err(rpc_err!("<Table as Service<TableRequest>>::poll_ready"))?;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: TableRequest) -> Self::Future {
        // TODO(eta): error handling impl adds overhead
        let tn = self.table_name.to_owned();
        match req {
            TableRequest::TableOperations(ops) => match self.prep_records(ops) {
                Ok(i) => future::Either::Left(future::Either::Left(
                    self.input(i).map_err(|e| table_err(tn, e)),
                )),
                Err(e) => future::Either::Left(future::Either::Right(async move { Err(e) })),
            },
            TableRequest::Timestamp(t) => {
                let p = PacketData {
                    dst: self.node,
                    data: PacketPayload::Timestamp(t),
                    trace: None,
                };
                future::Either::Right(self.timestamp(p).map_err(|e| table_err(tn, e)))
            }
        }
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

    fn inject_dropped_cols(&self, r: &mut TableOperation) -> ReadySetResult<()> {
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
            // TODO: what about updates? do we need to rewrite the set vector?

            // we want to be a bit careful here to avoid shifting elements multiple times. we
            // do this by moving from the back, and swapping the tail element to the end of the
            // vector until we hit each index.

            // in other words, if we have two default values, we're going to start out with:
            //
            // |####..|
            //
            // where # are "real" fields in the record and . are None values.
            // we want to end up with something like
            //
            // |#d##d#|
            //
            // if columns 1 and 4 were dropped (d here signifies the default values).
            // what makes this tricky is that we need to preserve the order of all the #.
            // to accomplish this, we're going to move the # to the end of the record, one at a
            // time, starting with the last one, and then "inject" the default values as we go.
            // that way, we only make one pass over the record!
            //
            // in particular, progress is going to look like this (i've swapped # for col #):
            //
            // |1234..|  hole = 5, next_insert = 4, last_unmoved = 3
            // swap 4 and last .
            // |123..4|  hole = 4, next_insert = 4, last_unmoved = 2
            // hole == next_insert, so insert default value
            // |123.d4|  hole = 4, next_insert = 4, last_unmoved = 2
            // move on to next dropped column
            // |123.d4|  hole = 3, next_insert = 1, last_unmoved = 2
            // swap 3 and last .
            // |12.3d4|  hole = 2, next_insert = 1, last_unmoved = 1
            // swap 2 and last .
            // |1.23d4|  hole = 1, next_insert = 1, last_unmoved = 0
            // hole == next_insert, so insert default value
            // |1d23d4|
            // move on to next dropped column, but since there is none, we're done

            // make room in the record
            let n = r.len() + ndropped;
            let mut hole = n;
            let mut last_unmoved = r.len() - 1;
            r.resize(n, DataType::None);

            // keep trying to insert the next dropped column
            for (next_insert, default) in dropped {
                // think of this being at the bottom of the loop
                // we just hoist it here to avoid underflow if we ever insert at 0
                hole -= 1;

                // shift elements until the next free slot is the one we want to insert into
                while hole != next_insert {
                    // shift another element so the free slot is at a lower index
                    r.swap(last_unmoved, hole);
                    hole -= 1;

                    if last_unmoved == 0 {
                        // there are no more elements -- the next slot to insert at better be [0]
                        debug_assert_eq!(next_insert, 0);
                        debug_assert_eq!(hole, 0);
                        break;
                    }
                    last_unmoved -= 1;
                }

                // we're at the right index -- insert the dropped value
                let current = match r.get_mut(next_insert) {
                    Some(v) => v,
                    None => internal!("index out of bounds"),
                };
                let old = mem::replace(current, default.clone());
                debug_assert_eq!(old, DataType::None);
            }
        }
        Ok(())
    }

    /// Generates a PacketTrace object every TRACE_SAMPLE_RATE. This performs
    /// head based sampling, informing downstream nodes that process packets
    /// to record trace info for all packets generated from this Input
    /// packet.
    fn generate_trace_info(&mut self) -> Option<PacketTrace> {
        let now = Instant::now();
        // If we have already sent a trace for a packet in the last TRACE_SAMPLE_RATE,
        // do not include trace info in the packet.
        if now - self.last_trace_sample < TRACE_SAMPLE_RATE {
            return None;
        }

        self.last_trace_sample = now;
        Some(PacketTrace {
            start: SystemTime::now(),
        })
    }

    fn prep_records(&mut self, mut ops: Vec<TableOperation>) -> ReadySetResult<PacketData> {
        for r in &mut ops {
            self.inject_dropped_cols(r)?;
        }

        Ok(PacketData {
            dst: self.node,
            data: PacketPayload::Input(ops),
            trace: self.generate_trace_info(),
        })
    }

    async fn quick_n_dirty<Request, R>(
        &mut self,
        r: Request,
    ) -> Result<R, <Self as Service<Request>>::Error>
    where
        Request: Send + 'static,
        Self: Service<Request, Response = Tagged<R>>,
    {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;
        Ok(self.call(r).await?.v)
    }

    async fn quick_n_dirty_with_timeout(&mut self, r: TableRequest) -> ReadySetResult<()> {
        Ok(
            tokio::time::timeout(self.request_timeout, self.quick_n_dirty(r))
                .await
                .map_err(|_| internal_err("Timeout during table request"))??,
        )
    }

    /// Insert a single row of data into this base table.
    pub async fn insert<V>(&mut self, u: V) -> ReadySetResult<()>
    where
        V: Into<Vec<DataType>>,
    {
        self.quick_n_dirty(TableRequest::TableOperations(vec![TableOperation::Insert(
            u.into(),
        )]))
        .await
    }

    /// Insert multiple rows of data into this base table.
    pub async fn insert_many<I, V>(&mut self, rows: I) -> ReadySetResult<()>
    where
        I: IntoIterator<Item = V>,
        V: Into<Vec<DataType>>,
    {
        self.quick_n_dirty_with_timeout(TableRequest::TableOperations(
            rows.into_iter()
                .map(|row| TableOperation::Insert(row.into()))
                .collect::<Vec<_>>(),
        ))
        .await
    }

    /// Perform multiple operation on this base table.
    pub async fn perform_all<I, V>(&mut self, i: I) -> ReadySetResult<()>
    where
        I: IntoIterator<Item = V>,
        V: Into<TableOperation>,
    {
        self.quick_n_dirty_with_timeout(TableRequest::TableOperations(
            i.into_iter().map(Into::into).collect::<Vec<_>>(),
        ))
        .await
    }

    /// Delete the row with the given key from this base table.
    pub async fn delete<I>(&mut self, key: I) -> ReadySetResult<()>
    where
        I: Into<Vec<DataType>>,
    {
        self.quick_n_dirty_with_timeout(TableRequest::TableOperations(vec![
            TableOperation::DeleteByKey { key: key.into() },
        ]))
        .await
    }

    /// Delete one occurrence of the row matching the *entirety* of the given row from the base
    /// table.
    pub async fn delete_row<I>(&mut self, row: I) -> ReadySetResult<()>
    where
        I: Into<Vec<DataType>>,
    {
        self.quick_n_dirty_with_timeout(TableRequest::TableOperations(vec![
            TableOperation::DeleteRow { row: row.into() },
        ]))
        .await
    }

    /// Update the row with the given key in this base table.
    ///
    /// `u` is a set of column-modification pairs, where for each pair `(i, m)`, the modification
    /// `m` will be applied to column `i` of the record with key `key`.
    pub async fn update<V>(&mut self, key: Vec<DataType>, u: V) -> ReadySetResult<()>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        if self.key.is_empty() || !self.key_is_primary {
            unsupported!("update operations can only be applied to base nodes with key columns")
        }

        let mut update = vec![Modification::None; self.columns.len()];
        for (coli, m) in u {
            match update.get_mut(coli) {
                Some(elem) => *elem = m,
                None => {
                    return Err(table_err(
                        self.table_name(),
                        ReadySetError::WrongColumnCount(self.columns.len(), coli + 1),
                    ))
                }
            }
        }

        self.quick_n_dirty_with_timeout(TableRequest::TableOperations(vec![
            TableOperation::Update { key, update },
        ]))
        .await
    }

    /// Perform a insert-or-update on this base table.
    ///
    /// If a row already exists for the key in `insert`, the existing row will instead be updated
    /// with the modifications in `u` (as documented in `Table::update`).
    pub async fn insert_or_update<V>(
        &mut self,
        insert: Vec<DataType>,
        update: V,
    ) -> ReadySetResult<()>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        if self.key.is_empty() || !self.key_is_primary {
            unsupported!("update operations can only be applied to base nodes with key columns")
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in update {
            match set.get_mut(coli) {
                Some(elem) => *elem = m,
                None => {
                    return Err(table_err(
                        self.table_name(),
                        ReadySetError::WrongColumnCount(self.columns.len(), coli + 1),
                    ))
                }
            }
        }

        self.quick_n_dirty_with_timeout(TableRequest::TableOperations(vec![
            TableOperation::InsertOrUpdate {
                row: insert,
                update: set,
            },
        ]))
        .await
    }

    /// Updates the timestamp of the base table in the data flow graph.
    pub async fn update_timestamp(&mut self, t: consistency::Timestamp) -> ReadySetResult<()> {
        self.quick_n_dirty_with_timeout(TableRequest::Timestamp(t))
            .await
    }

    /// Set the replication offset for this table to the given value.
    ///
    /// Generally this method should not be used, instead preferring to atomically set replication
    /// offsets as part of an existing write batch - but there are some cases where it might be
    /// useful to set outside of a write batch, such as in tests.
    ///
    /// See [the documentation for PersistentState](::noria_dataflow::state::persistent_state) for
    /// more information about replication offsets.
    pub async fn set_replication_offset(
        &mut self,
        offset: ReplicationOffset,
    ) -> ReadySetResult<()> {
        self.quick_n_dirty(TableRequest::TableOperations(vec![
            TableOperation::SetReplicationOffset(offset),
        ]))
        .await
    }

    /// Enable or disable snapshot mode for this table. In snapshot mode compactions are disabled
    /// and writes don't go into WAL.
    pub async fn set_snapshot_mode(&mut self, snapshot: bool) -> ReadySetResult<()> {
        self.quick_n_dirty(TableRequest::TableOperations(vec![
            TableOperation::SetSnapshotMode(snapshot),
        ]))
        .await
    }
}
