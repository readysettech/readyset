use byteorder::WriteBytesExt;
use myc::constants::{ColumnFlags, StatusFlags};
use packet::PacketWriter;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::{self, Write};
use value::ToMysqlValue;
use writers;
use {Column, ErrorKind, StatementData};

/// Convenience type for responding to a client `USE <db>` command.
pub struct InitWriter<'a, W: Write + 'a> {
    pub(crate) writer: &'a mut PacketWriter<W>,
}

impl<'a, W: Write + 'a> InitWriter<'a, W> {
    /// Tell client that database context has been changed
    pub fn ok(self) -> io::Result<()> {
        writers::write_ok_packet(self.writer, 0, 0, StatusFlags::empty())
    }

    /// Tell client that there was a problem changing the database context.
    /// Although you can return any valid MySQL error code you probably want
    /// to keep it similar to the MySQL server and issue either a
    /// `ErrorKind::ER_BAD_DB_ERROR` or a `ErrorKind::ER_DBACCESS_DENIED_ERROR`.
    pub fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        writers::write_err(kind, msg.borrow(), self.writer)
    }
}

/// Convenience type for responding to a client `PREPARE` command.
///
/// This type should not be dropped without calling
/// [`reply`](struct.StatementMetaWriter.html#method.reply) or
/// [`error`](struct.StatementMetaWriter.html#method.error).
#[must_use]
pub struct StatementMetaWriter<'a, W: Write + 'a> {
    pub(crate) writer: &'a mut PacketWriter<W>,
    pub(crate) stmts: &'a mut HashMap<u32, StatementData>,
}

impl<'a, W: Write + 'a> StatementMetaWriter<'a, W> {
    /// Reply to the client with the given meta-information.
    ///
    /// `id` is a statement identifier that the client should supply when it later wants to execute
    /// this statement. `params` is a set of [`Column`](struct.Column.html) descriptors for the
    /// parameters the client must provide when executing the prepared statement. `columns` is a
    /// second set of [`Column`](struct.Column.html) descriptors for the values that will be
    /// returned in each row then the statement is later executed.
    pub fn reply<PI, CI>(self, id: u32, params: PI, columns: CI) -> io::Result<()>
    where
        PI: IntoIterator<Item = &'a Column>,
        CI: IntoIterator<Item = &'a Column>,
        <PI as IntoIterator>::IntoIter: ExactSizeIterator,
        <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let params = params.into_iter();
        self.stmts.insert(
            id,
            StatementData {
                params: params.len() as u16,
                ..Default::default()
            },
        );
        writers::write_prepare_ok(id, params, columns, self.writer)
    }

    /// Reply to the client's `PREPARE` with an error.
    pub fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        writers::write_err(kind, msg.borrow(), self.writer)
    }
}

enum Finalizer {
    Ok { rows: u64, last_insert_id: u64 },
    EOF,
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
pub struct QueryResultWriter<'a, W: Write + 'a> {
    // XXX: specialization instead?
    pub(crate) is_bin: bool,
    pub(crate) writer: &'a mut PacketWriter<W>,
    last_end: Option<Finalizer>,
}

impl<'a, W: Write> QueryResultWriter<'a, W> {
    pub(crate) fn new(writer: &'a mut PacketWriter<W>, is_bin: bool) -> Self {
        QueryResultWriter {
            is_bin,
            writer,
            last_end: None,
        }
    }

    fn finalize(&mut self, more_exists: bool) -> io::Result<()> {
        let mut status = StatusFlags::empty();
        if more_exists {
            status.set(StatusFlags::SERVER_MORE_RESULTS_EXISTS, true);
        }
        match self.last_end.take() {
            None => Ok(()),
            Some(Finalizer::Ok {
                rows,
                last_insert_id,
            }) => writers::write_ok_packet(self.writer, rows, last_insert_id, status),
            Some(Finalizer::EOF) => writers::write_eof_packet(self.writer, status),
        }
    }

    /// Start a resultset response to the client that conforms to the given `columns`.
    ///
    /// Note that if no columns are emitted, any written rows are ignored.
    ///
    /// See [`RowWriter`](struct.RowWriter.html).
    pub fn start(mut self, columns: &'a [Column]) -> io::Result<RowWriter<'a, W>> {
        self.finalize(true)?;
        RowWriter::new(self, columns)
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query in this resultset. `last_insert_id` may be given to communiate an identifier for
    /// a client's most recent insertion.
    pub fn complete_one(mut self, rows: u64, last_insert_id: u64) -> io::Result<Self> {
        self.finalize(true)?;
        self.last_end = Some(Finalizer::Ok {
            rows,
            last_insert_id,
        });
        Ok(self)
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query. `last_insert_id` may be given to communiate an identifier for a client's most
    /// recent insertion.
    pub fn completed(self, rows: u64, last_insert_id: u64) -> io::Result<()> {
        self.complete_one(rows, last_insert_id)?.no_more_results()
    }

    /// Reply to the client's query with an error.
    pub fn error<E>(mut self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        self.finalize(true)?;
        writers::write_err(kind, msg.borrow(), self.writer)
    }

    /// Send the last bits of the last resultset to the client, and indicate that there are no more
    /// resultsets coming.
    pub fn no_more_results(mut self) -> io::Result<()> {
        self.finalize(false)
    }
}

impl<'a, W: Write> Drop for QueryResultWriter<'a, W> {
    fn drop(&mut self) {
        self.finalize(false).unwrap();
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
pub struct RowWriter<'a, W: Write + 'a> {
    result: Option<QueryResultWriter<'a, W>>,
    bitmap_len: usize,
    data: Vec<u8>,
    columns: &'a [Column],

    // next column to write for the current row
    // NOTE: (ab)used to track number of *rows* for a zero-column resultset
    col: usize,

    finished: bool,
}

impl<'a, W> RowWriter<'a, W>
where
    W: Write + 'a,
{
    fn new(
        result: QueryResultWriter<'a, W>,
        columns: &'a [Column],
    ) -> io::Result<RowWriter<'a, W>> {
        let bitmap_len = (columns.len() + 7 + 2) / 8;
        let mut rw = RowWriter {
            result: Some(result),
            columns,
            bitmap_len,
            data: Vec::new(),

            col: 0,

            finished: false,
        };
        rw.start()?;
        Ok(rw)
    }

    #[inline]
    fn start(&mut self) -> io::Result<()> {
        if !self.columns.is_empty() {
            writers::column_definitions(self.columns, self.result.as_mut().unwrap().writer)?;
        }
        Ok(())
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
        T: ToMysqlValue,
    {
        if self.columns.is_empty() {
            return Ok(());
        }

        if self.result.as_mut().unwrap().is_bin {
            if self.col == 0 {
                self.result.as_mut().unwrap().writer.write_u8(0x00)?;

                // leave space for nullmap
                self.data.resize(self.bitmap_len, 0);
            }

            let c = self
                .columns
                .get(self.col)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "row has more columns than specification",
                    )
                })?
                .borrow();
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
                    self.data[(self.col + 2) / 8] |= 1u8 << ((self.col + 2) % 8);
                }
            } else {
                v.to_mysql_bin(&mut self.data, c)?;
            }
        } else {
            v.to_mysql_text(self.result.as_mut().unwrap().writer)?;
        }
        self.col += 1;
        Ok(())
    }

    /// Indicate that no more column data will be written for the current row.
    pub fn end_row(&mut self) -> io::Result<()> {
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

        if self.result.as_mut().unwrap().is_bin {
            self.result
                .as_mut()
                .unwrap()
                .writer
                .write_all(&self.data[..])?;
            self.data.clear();
        }
        self.result.as_mut().unwrap().writer.end_packet()?;
        self.col = 0;

        Ok(())
    }

    /// Write a single row as a part of this resultset.
    ///
    /// Note that the row *must* conform to the column specification provided to
    /// [`QueryResultWriter::start`](struct.QueryResultWriter.html#method.start). If it does not,
    /// this method will return an error indicating that an invalid value type or specification was
    /// provided.
    pub fn write_row<I, E>(&mut self, row: I) -> io::Result<()>
    where
        I: IntoIterator<Item = E>,
        E: ToMysqlValue,
    {
        if !self.columns.is_empty() {
            for v in row {
                self.write_col(v)?;
            }
        }
        self.end_row()
    }
}

impl<'a, W: Write + 'a> RowWriter<'a, W> {
    fn finish_inner(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;

        if !self.columns.is_empty() && self.col != 0 {
            self.end_row()?;
        }

        if self.columns.is_empty() {
            // response to no column query is always an OK packet
            // we've kept track of the number of rows in col (hacky, I know)
            self.result.as_mut().unwrap().last_end = Some(Finalizer::Ok {
                rows: self.col as u64,
                last_insert_id: 0,
            });
        } else {
            // we wrote out at least one row
            self.result.as_mut().unwrap().last_end = Some(Finalizer::EOF);
        }
        Ok(())
    }

    /// Indicate to the client that no more rows are coming.
    pub fn finish(self) -> io::Result<()> {
        self.finish_one()?.no_more_results()
    }

    /// End this resultset response, and indicate to the client that no more rows are coming.
    pub fn finish_one(mut self) -> io::Result<QueryResultWriter<'a, W>> {
        self.finish_inner()?;
        // we know that dropping self will see self.finished == true,
        // and so Drop won't try to use self.result.
        Ok(self.result.take().unwrap())
    }
}

impl<'a, W: Write + 'a> Drop for RowWriter<'a, W> {
    fn drop(&mut self) {
        self.finish_inner().unwrap();
    }
}
