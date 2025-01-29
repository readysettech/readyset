use std::borrow::Borrow;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use readyset_adapter_types::StatementId;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::myc::constants::{ColumnFlags, StatusFlags};
use crate::packet::{PacketConn, MAX_PACKET_CHUNK_SIZE};
use crate::value::ToMySqlValue;
use crate::{writers, Column, ErrorKind, StatementData};

pub(crate) const DEFAULT_ROW_CAPACITY: usize = 4096;
pub(crate) const MAX_POOL_ROW_CAPACITY: usize = DEFAULT_ROW_CAPACITY * 4;

/// Convenience type for responding to a client `USE <db>` command.
pub struct InitWriter<'a, S: AsyncRead + AsyncWrite + Unpin> {
    pub(crate) conn: &'a mut PacketConn<S>,
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin + 'a> InitWriter<'a, S> {
    /// Tell client that database context has been changed
    pub async fn ok(self) -> io::Result<()> {
        writers::write_ok_packet(self.conn, 0, 0, StatusFlags::empty()).await
    }

    /// Tell client that there was a problem changing the database context.
    /// Although you can return any valid MySQL error code you probably want
    /// to keep it similar to the MySQL server and issue either a
    /// `ErrorKind::ER_BAD_DB_ERROR` or a `ErrorKind::ER_DBACCESS_DENIED_ERROR`.
    pub async fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        writers::write_err(kind, msg.borrow(), self.conn).await
    }
}

/// Convenience type for responding to a client `PREPARE` command.
///
/// This type should not be dropped without calling
/// [`reply`](struct.StatementMetaWriter.html#method.reply) or
/// [`error`](struct.StatementMetaWriter.html#method.error).
#[must_use]
pub struct StatementMetaWriter<'a, S: AsyncRead + AsyncWrite + Unpin> {
    pub(crate) conn: &'a mut PacketConn<S>,
    pub(crate) stmts: &'a mut HashMap<StatementId, StatementData>,
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin + 'a> StatementMetaWriter<'a, S> {
    /// Reply to the client with the given meta-information.
    ///
    /// `id` is a statement identifier that the client should supply when it later wants to execute
    /// this statement. `params` is a set of [`Column`](struct.Column.html) descriptors for the
    /// parameters the client must provide when executing the prepared statement. `columns` is a
    /// second set of [`Column`](struct.Column.html) descriptors for the values that will be
    /// returned in each row then the statement is later executed.
    pub async fn reply<PI, CI>(self, id: StatementId, params: PI, columns: CI) -> io::Result<()>
    where
        PI: IntoIterator<Item = &'a Column>,
        CI: IntoIterator<Item = &'a Column>,
        <PI as IntoIterator>::IntoIter: ExactSizeIterator,
        <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let id_named = match id {
            StatementId::Named(id) => id,
            StatementId::Unnamed => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Mysql does not support unnamed statements",
                ))
            }
        };
        let params = params.into_iter();
        self.stmts.insert(
            id,
            StatementData {
                params: params.len() as u16,
                ..Default::default()
            },
        );
        writers::write_prepare_ok(id_named, params, columns, self.conn).await
    }

    /// Reply to the client's `PREPARE` with an error.
    pub async fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        writers::write_err(kind, msg.borrow(), self.conn).await
    }
}

#[derive(Debug)]
enum Finalizer {
    Ok {
        rows: u64,
        last_insert_id: u64,
        status_flags: Option<StatusFlags>,
    },
    Eof {
        status_flags: Option<StatusFlags>,
    },
}

/// Convenience type for providing query results to clients.
///
/// This type should not be dropped without calling
/// [`start`](struct.QueryResultWriter.html#method.start),
/// [`completed`](struct.QueryResultWriter.html#method.completed), or
/// [`error`](struct.QueryResultWriter.html#method.error).
///
/// To send multiple resultsets, use
/// [`RowWriter::finish_one`](struct.RowWriter.html#method.finish_one) and
/// [`complete_one`](struct.QueryResultWriter.html#method.complete_one). These are similar to
/// `RowWriter::finish` and `completed`, but both eventually yield back the `QueryResultWriter` so
/// that another resultset can be sent. To indicate that no more resultset will be sent, call
/// [`no_more_results`](struct.QueryResultWriter.html#method.no_more_results). All methods on
/// `QueryResultWriter` (except `no_more_results`) automatically start a new resultset. The
/// `QueryResultWriter` *may* be dropped without calling `no_more_results`, but in this case the
/// program may panic if an I/O error occurs when sending the end-of-records marker to the client.
/// To handle such errors, call `no_more_results` explicitly.
#[must_use]
pub struct QueryResultWriter<'a, S: AsyncRead + AsyncWrite + Unpin> {
    // XXX: specialization instead?
    pub(crate) is_bin: bool,
    pub(crate) conn: &'a mut PacketConn<S>,
    last_end: Option<Finalizer>,
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin> QueryResultWriter<'a, S> {
    pub(crate) fn new(conn: &'a mut PacketConn<S>, is_bin: bool) -> Self {
        QueryResultWriter {
            is_bin,
            conn,
            last_end: None,
        }
    }

    async fn finalize(&mut self, more_exists: bool) -> io::Result<()> {
        let mut status = match self.last_end {
            Some(Finalizer::Ok {
                rows: _,
                last_insert_id: _,
                status_flags,
            })
            | Some(Finalizer::Eof { status_flags }) => {
                if let Some(sf) = status_flags {
                    sf
                } else {
                    StatusFlags::empty()
                }
            }
            _ => StatusFlags::empty(),
        };
        if more_exists {
            status.set(StatusFlags::SERVER_MORE_RESULTS_EXISTS, true);
        }
        match self.last_end.take() {
            None => Ok(()),
            Some(Finalizer::Ok {
                rows,
                last_insert_id,
                ..
            }) => writers::write_ok_packet(self.conn, rows, last_insert_id, status).await,
            Some(Finalizer::Eof { .. }) => writers::write_eof_packet(self.conn, status).await,
        }
    }

    /// Start a resultset response to the client that conforms to the given `columns`.
    ///
    /// Note that if no columns are emitted, any written rows are ignored.
    ///
    /// See [`RowWriter`](struct.RowWriter.html).
    pub async fn start(mut self, columns: &'a [Column]) -> io::Result<RowWriter<'a, S>> {
        self.finalize(true).await?;
        RowWriter::new(self, columns, None).await
    }

    /// Start a resultset response to the client that conforms to the given `columns`.
    /// It accepts a preencoded representation of the `columns` that can be directly
    /// emitted to the wire as an optimization.
    ///
    /// Note that if no columns are emitted, any written rows are ignored.
    ///
    /// See [`RowWriter`](struct.RowWriter.html).
    pub async fn start_with_cache(
        mut self,
        columns: &'a [Column],
        cached: Arc<[u8]>,
    ) -> io::Result<RowWriter<'a, S>> {
        self.finalize(true).await?;
        RowWriter::new(self, columns, Some(cached)).await
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query in this resultset. `last_insert_id` may be given to communiate an identifier for
    /// a client's most recent insertion.
    pub async fn complete_one(
        mut self,
        rows: u64,
        last_insert_id: u64,
        status_flags: Option<StatusFlags>,
        // return type not Self because https://github.com/rust-lang/rust/issues/61949
    ) -> io::Result<QueryResultWriter<'a, S>> {
        self.finalize(true).await?;
        self.last_end = Some(Finalizer::Ok {
            rows,
            last_insert_id,
            status_flags,
        });
        Ok(self)
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query. `last_insert_id` may be given to communiate an identifier for a client's most
    /// recent insertion.
    pub async fn completed(
        self,
        rows: u64,
        last_insert_id: u64,
        status_flags: Option<StatusFlags>,
    ) -> io::Result<()> {
        self.complete_one(rows, last_insert_id, status_flags)
            .await?
            .no_more_results()
            .await
    }

    /// Reply to the client's query with an error.
    ///
    /// This also calls `no_more_results` implicitly.
    pub async fn error<E>(mut self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        self.finalize(true).await?;
        writers::write_err(kind, msg.borrow(), self.conn).await?;
        self.no_more_results().await
    }

    /// Send the last bits of the last resultset to the client, and indicate that there are no more
    /// resultsets coming.
    pub async fn no_more_results(mut self) -> io::Result<()> {
        self.finalize(false).await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Drop for QueryResultWriter<'_, S> {
    fn drop(&mut self) {
        if let Some(x) = self.last_end.take() {
            eprintln!(
                "WARNING(mysql-srv): QueryResultWriter dropped without finalizing {:?}",
                x
            );
        }
    }
}

/// Convenience type for sending rows of a resultset to a client.
///
/// Rows can either be written out one column at a time (using
/// [`write_col`](struct.RowWriter.html#method.write_col) and
/// [`end_row`](struct.RowWriter.html#method.end_row)), or one row at a time (using
/// [`write_row`](struct.RowWriter.html#method.write_row)).
///
/// This type *may* be dropped without calling
/// [`write_row`](struct.RowWriter.html#method.write_row) or
/// [`finish`](struct.RowWriter.html#method.finish). However, in this case, the program may panic
/// if an I/O error occurs when sending the end-of-records marker to the client. To avoid this,
/// call [`finish`](struct.RowWriter.html#method.finish) explicitly.
#[must_use]
pub struct RowWriter<'a, S: AsyncRead + AsyncWrite + Unpin> {
    result: QueryResultWriter<'a, S>,
    bitmap_len: usize,
    /// The index where the null bitmap for the current row begins
    bitmap_idx: usize,
    columns: &'a [Column],
    /// A cached pre-encoded representation of the column definitions
    cached: Option<Arc<[u8]>>,

    // next column to write for the current row
    // NOTE: (ab)used to track number of *rows* for a zero-column resultset
    col: usize,

    finished: bool,
    // Optionally holds the status flags from the last ok packet that we have
    // received from communicating with mysql over fallback.
    last_status_flags: Option<StatusFlags>,
    /// A buffer to hold row data
    row_data: Option<Vec<u8>>,

    // the index of the current row header, so that we can go back and update it
    // after we've written the row data.
    cur_row_header_idx: usize,
}

impl<'a, S> RowWriter<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + 'a,
{
    async fn new(
        result: QueryResultWriter<'a, S>,
        columns: &'a [Column],
        cached_column_def: Option<Arc<[u8]>>,
    ) -> io::Result<RowWriter<'a, S>> {
        let bitmap_len = (columns.len() + 7 + 2) / 8;
        let mut rw = RowWriter {
            result,
            columns,
            cached: cached_column_def,
            bitmap_len,
            bitmap_idx: 0,

            col: 0,

            finished: false,
            last_status_flags: None,

            row_data: None,
            cur_row_header_idx: 0,
        };
        rw.start().await?;
        Ok(rw)
    }

    async fn start(&mut self) -> io::Result<()> {
        if self.columns.is_empty() {
            return Ok(());
        }

        match &self.cached {
            Some(cached) => {
                writers::column_definitions_cached(self.columns, cached.clone(), self.result.conn)
                    .await
            }
            None => writers::column_definitions(self.columns, self.result.conn).await,
        }
    }

    /// Write a value to the next column of the current row as a part of this resultset.
    ///
    /// If you do not call [`end_row`](struct.RowWriter.html#method.end_row) after the last row,
    /// any errors that occur when writing out the last row will be returned by
    /// [`finish`](struct.RowWriter.html#method.finish). If you do not call `finish` either, any
    /// errors will cause a panic when the `RowWriter` is dropped.
    ///
    /// Note that the row *must* conform to the column specification provided to
    /// [`QueryResultWriter::start`](struct.QueryResultWriter.html#method.start). If it does not,
    /// this method will return an error indicating that an invalid value type or specification was
    /// provided.
    pub fn write_col<T>(&mut self, v: T) -> io::Result<()>
    where
        T: ToMySqlValue,
    {
        if self.columns.is_empty() {
            return Ok(());
        }

        let row_data = self.row_data.get_or_insert_with(|| {
            let mut row_data = self.result.conn.get_buffer();
            // We want to preallocate at least *some* capacity for the row, otherwise the
            // incremental writes cause a whole lot of reallocations. Since Vec usually
            // reallocates with exponential growth even small responses require at least
            // a few reallocs unless we reserve some capacity.
            row_data.reserve(DEFAULT_ROW_CAPACITY);
            row_data
        });

        // set the packet header. the header is the first 4 bytes of the packet: 3 bytes for size
        // and 1 byte for sequence number. We don't know the size yet, so we'll set it later.
        if self.col == 0 {
            self.cur_row_header_idx = row_data.len();
            // The mysql header is 4 bytes. We insert a placeholder for now, until we know the size (which will be set in `end_row()`).
            let hdr = 0_u32.to_be_bytes();
            row_data.extend_from_slice(&hdr);
        }

        if self.result.is_bin {
            if self.col == 0 {
                row_data.push(0x00);
                // leave space for nullmap
                self.bitmap_idx = row_data.len();
                row_data.resize(row_data.len() + self.bitmap_len, 0);
            }

            let c = self.columns.get(self.col).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "row has more columns than specification",
                )
            })?;

            if v.is_null() {
                if c.colflags.contains(ColumnFlags::NOT_NULL_FLAG) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "given NULL value for NOT NULL column",
                    ));
                } else {
                    // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
                    // NULL-bitmap-byte = ((field-pos + offset) / 8)
                    // NULL-bitmap-bit  = ((field-pos + offset) % 8)
                    let idx = self.bitmap_idx + (self.col + 2) / 8;
                    // Always safe to access `idx` because we allocate sufficient space in advance
                    row_data[idx] |= 1u8 << ((self.col + 2) % 8);
                }
            } else {
                v.to_mysql_bin(row_data, c)?;
            }
        } else {
            v.to_mysql_text(row_data)?;
        }
        self.col += 1;
        Ok(())
    }

    /// Write the packet header and enqueue the packet if it exceeds the max pool row capacity.
    #[inline]
    async fn finish_normal_packet(&mut self, packet_len: usize) -> io::Result<()> {
        let packet = self.row_data.as_mut().unwrap();
        let header_bytes = self.result.conn.packet_header_bytes(packet_len);
        packet[self.cur_row_header_idx..self.cur_row_header_idx + 4].copy_from_slice(&header_bytes);

        if packet.len() > MAX_POOL_ROW_CAPACITY {
            // Only take ownership when we need to enqueue
            self.result
                .conn
                .enqueue_plain(self.row_data.take().unwrap());
            self.result.conn.flush().await?;
        }
        Ok(())
    }

    /// Write the packet header and enqueue the packet if it exceeds the max pool row capacity.
    ///
    /// This is a bit tricky as we don't know if smaller rows have already been added to the buffer.
    /// If this is the first row inserted into the buffer, we can just send the packet (mostly) as-is
    /// to `enqueue_large_packet()`. If it's not the first row, we need to send the earlier rows
    /// first as they do not need to be chunked, then we send the rest of the buffer to
    /// `enqueue_large_packet()`.
    #[inline]
    async fn finish_large_packet(&mut self) -> io::Result<()> {
        // Take ownership as we need to enqueue
        let mut payload = self.row_data.take().unwrap();

        // this large packet is not the first row, so we need to send the earlier rows first as they do
        // not need to be chunked.
        if self.cur_row_header_idx > 0 {
            // we make the assumption that the first section, before the large packet, is smaller than the large packet.
            // thus, we copy that out to a new vector and enqueue it separately. Yes, the copy sucks, but this is a rare case.
            let first_part: Vec<u8> = payload.drain(..self.cur_row_header_idx).collect();
            self.result.conn.enqueue_plain(first_part);
            // payload now contains only the remaining (larger) part
        }

        // enqueue the rest of the packet, but we need to drop the first 4 bytes as they are the row header.
        payload.drain(..4);
        self.result.conn.enqueue_large_packet(payload);

        self.result.conn.flush().await
    }

    /// Indicate that no more column data will be written for the current row.
    pub async fn end_row(&mut self) -> io::Result<()> {
        if self.columns.is_empty() {
            self.col += 1;
            return Ok(());
        }

        if self.col != self.columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "row has fewer columns than specification",
            ));
        }

        if let Some(packet) = self.row_data.as_mut() {
            let packet_len = packet.len() - (self.cur_row_header_idx + 4);

            // if the packet is larger than the max packet size, we need to chunk it
            if packet_len < MAX_PACKET_CHUNK_SIZE {
                self.finish_normal_packet(packet_len).await?;
            } else {
                self.finish_large_packet().await?;
            }
        }

        self.col = 0;

        Ok(())
    }

    /// Write a single row as a part of this resultset.
    ///
    /// Note that the row *must* conform to the column specification provided to
    /// [`QueryResultWriter::start`](struct.QueryResultWriter.html#method.start). If it does not,
    /// this method will return an error indicating that an invalid value type or specification was
    /// provided.
    pub async fn write_row<I, E>(&mut self, row: I) -> io::Result<()>
    where
        I: IntoIterator<Item = E>,
        E: ToMySqlValue,
    {
        if !self.columns.is_empty() {
            for v in row {
                self.write_col(v)?;
            }
        }
        self.end_row().await
    }
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin + 'a> RowWriter<'a, S> {
    fn finish_inner(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;

        if self.columns.is_empty() {
            // response to no column query is always an OK packet
            // we've kept track of the number of rows in col (hacky, I know)
            self.result.last_end = Some(Finalizer::Ok {
                rows: self.col as u64,
                last_insert_id: 0,
                status_flags: self.last_status_flags.take(),
            });
            Ok(())
        } else {
            // we wrote out at least one row
            self.result.last_end = Some(Finalizer::Eof {
                status_flags: self.last_status_flags.take(),
            });
            Ok(())
        }
    }

    /// Sets status flags to be eventually written out when finish() gets called.
    pub fn set_status_flags(mut self, status_flags: StatusFlags) -> Self {
        self.last_status_flags = Some(status_flags);
        self
    }

    /// Reply to the client's query with an error.
    ///
    /// This also calls `no_more_results` implicitly.
    pub async fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        self.result.error(kind, msg).await
    }

    /// Indicate to the client that no more rows are coming.
    pub async fn finish(self) -> io::Result<()> {
        self.finish_one().await?.no_more_results().await
    }

    /// End this resultset response, and indicate to the client that no more rows are coming.
    pub async fn finish_one(mut self) -> io::Result<QueryResultWriter<'a, S>> {
        if !self.columns.is_empty() && self.col != 0 {
            self.end_row().await?;
        }

        // if there's any row data left, enqueue it.
        if let Some(packet) = self.row_data.take() {
            self.result.conn.enqueue_plain(packet);

            // as we will have at least an EOF/OK packet that follows the last row packet,
            // we don't need to flush here.
        }

        self.finish_inner()?;
        Ok(self.result)
    }
}
