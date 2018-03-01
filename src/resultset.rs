use std::io::{self, Write};
use packet::PacketWriter;
use writers;
use {Column, ErrorKind};
use std::borrow::Borrow;
use byteorder::WriteBytesExt;
use value::ToMysqlValue;
use myc::constants::{ColumnFlags, StatusFlags};

/// Convenience type for responding to a client `PREPARE` command.
///
/// This type should not be dropped without calling
/// [`reply`](struct.StatementMetaWriter.html#method.reply) or
/// [`error`](struct.StatementMetaWriter.html#method.error).
#[must_use]
pub struct StatementMetaWriter<'a, W: Write + 'a> {
    pub(crate) writer: &'a mut PacketWriter<W>,
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

/// Convenience type for providing query results to clients.
///
/// This type should not be dropped without calling
/// [`start`](struct.QueryResultWriter.html#method.start),
/// [`completed`](struct.QueryResultWriter.html#method.completed), or
/// [`error`](struct.QueryResultWriter.html#method.error).
#[must_use]
pub struct QueryResultWriter<'a, W: Write + 'a> {
    // XXX: specialization instead?
    pub(crate) is_bin: bool,
    pub(crate) writer: &'a mut PacketWriter<W>,
}

impl<'a, W: Write> QueryResultWriter<'a, W> {
    /// Start a resultset response to the client that conforms to the given `columns`.
    ///
    /// Note that if no columns are emitted, any written rows are ignored.
    ///
    /// See [`RowWriter`](struct.RowWriter.html).
    pub fn start(self, columns: &'a [Column]) -> io::Result<RowWriter<'a, W>> {
        RowWriter::new(self.writer, columns, self.is_bin)
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query. `last_insert_id` may be given to communiate an identifier for a client's most
    /// recent insertion.
    pub fn completed(self, rows: u64, last_insert_id: u64) -> io::Result<()> {
        writers::write_ok_packet(self.writer, rows, last_insert_id)
    }

    /// Reply to the client's query with an error.
    pub fn error<E>(self, kind: ErrorKind, msg: &E) -> io::Result<()>
    where
        E: Borrow<[u8]> + ?Sized,
    {
        writers::write_err(kind, msg.borrow(), self.writer)
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
    // XXX: specialization instead?
    is_bin: bool,
    writer: &'a mut PacketWriter<W>,
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
        writer: &'a mut PacketWriter<W>,
        columns: &'a [Column],
        is_bin: bool,
    ) -> io::Result<RowWriter<'a, W>> {
        let bitmap_len = (columns.len() + 7 + 2) / 8;
        let mut rw = RowWriter {
            is_bin: is_bin,
            writer: writer,
            columns: columns,
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
            writers::column_definitions(self.columns, self.writer)?;
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

        if self.is_bin {
            if self.col == 0 {
                self.writer.write_u8(0x00)?;

                // leave space for nullmap
                self.data.resize(self.bitmap_len, 0);
            }

            let c = self.columns
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
            v.to_mysql_text(self.writer)?;
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

        if self.is_bin {
            self.writer.write_all(&self.data[..])?;
            self.data.clear();
        }
        self.writer.end_packet()?;
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
        self.finished = true;

        if !self.columns.is_empty() && self.col != 0 {
            self.end_row()?;
        }

        if self.columns.is_empty() {
            // response to no column query is always an OK packet
            // we've kept track of the number of rows in col (hacky, I know)
            writers::write_ok_packet(self.writer, self.col as u64, 0)
        } else {
            // we wrote out at least one row
            writers::write_eof_packet(self.writer, StatusFlags::empty())
        }
    }

    /// End this resultset response, and indicate to the client that no more rows are coming.
    pub fn finish(mut self) -> io::Result<()> {
        self.finish_inner()
    }
}

impl<'a, W: Write + 'a> Drop for RowWriter<'a, W> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_inner().unwrap();
        }
    }
}
