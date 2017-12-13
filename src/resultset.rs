use std::io::{self, Write};
use packet::PacketWriter;
use writers;
use Column;
use bit_vec::BitVec;
use std::borrow::Borrow;
use byteorder::WriteBytesExt;
use value::ToMysqlValue;
use myc::io::WriteMysqlExt;
use myc::constants::{ColumnFlags, StatusFlags};

/// Convenience type for responding to a client `PREPARE` command.
///
/// This type should not be dropped without calling `reply`.
#[must_use]
pub struct StatementMetaWriter<'a, W: Write + 'a> {
    pub(crate) writer: &'a mut PacketWriter<W>,
}

impl<'a, W: Write + 'a> StatementMetaWriter<'a, W> {
    /// Reply to the client with the given meta-information.
    ///
    /// `id` is a statement identifier that the client should supply when it later wants to execute
    /// this statement. `params` is a set of `Column` descriptors for the parameters the client
    /// must provide when executing the prepared statement. `columns` is a set of `Column`
    /// descriptors for the values that will be returned in each row then the statement is later
    /// executed.
    pub fn reply<PI, CI>(self, id: u32, params: PI, columns: CI) -> io::Result<()>
    where
        PI: IntoIterator<Item = &'a Column>,
        CI: IntoIterator<Item = &'a Column>,
        <PI as IntoIterator>::IntoIter: ExactSizeIterator,
        <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        writers::write_prepare_ok(id, params, columns, self.writer)
    }
}

/// Convenience type for providing query results to clients.
///
/// This type should not be dropped without calling either `start` or `completed`.
#[must_use]
pub struct QueryResultWriter<'a, W: Write + 'a> {
    // XXX: specialization instead?
    pub(crate) is_bin: bool,
    pub(crate) writer: &'a mut PacketWriter<W>,
}

impl<'a, W: Write> QueryResultWriter<'a, W> {
    /// Start a resultset response to the client that conforms to the given `columns`.
    ///
    /// See `RowWriter`.
    pub fn start(self, columns: &'a [Column]) -> io::Result<RowWriter<'a, W>> {
        RowWriter::new(self.writer, columns, self.is_bin)
    }

    /// Send an empty resultset response to the client indicating that `rows` rows were affected by
    /// the query. `last_insert_id` may be given to communiate an identifier for a client's most
    /// recent insertion.
    pub fn completed(self, rows: u64, last_insert_id: u64) -> io::Result<()> {
        self.writer.write_u8(0x00)?; // OK packet type
        self.writer.write_lenenc_int(rows)?;
        self.writer.write_lenenc_int(last_insert_id)?;
        self.writer.write_all(&[0x00, 0x00])?; // no server status
        self.writer.write_all(&[0x00, 0x00])?; // no warnings
        Ok(())
    }
}

/// Convenience type for sending rows of a resultset to a client.
///
/// This type *may* be dropped without calling `write_row` or `finish`. However, in this case, the
/// program may panic if an I/O error occurs when sending the end-of-records marker to the client.
/// To avoid this, call `finish` explicitly.
#[must_use]
pub struct RowWriter<'a, W: Write + 'a> {
    // XXX: specialization instead?
    is_bin: bool,
    writer: &'a mut PacketWriter<W>,
    nullmap: BitVec<u8>,
    bitmap_len: usize,
    data: Vec<u8>,
    columns: &'a [Column],

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
        writers::column_definitions(columns, writer)?;

        let mut nullmap = BitVec::<u8>::default();
        nullmap.grow(columns.len() + 9, false);
        let bitmap_len = nullmap.blocks().count();

        Ok(RowWriter {
            is_bin: is_bin,
            writer: writer,
            columns: columns,
            nullmap,
            bitmap_len,
            data: Vec::new(),
            finished: false,
        })
    }

    /// Write a single row as a part of this resultset.
    ///
    /// Note that the row *must* conform to the column specification provided to
    /// `QueryResultWriter::start`. If it does not, this method will return an error indicating
    /// that an invalid value type or specification was provided.
    pub fn write_row<I, E>(&mut self, row: I) -> io::Result<()>
    where
        I: IntoIterator<Item = E>,
        E: ToMysqlValue,
    {
        if self.is_bin {
            //self.writer.write_u8(0x00)?;
            self.nullmap.clear();

            // leave space for nullmap
            self.data.resize(self.bitmap_len, 0);

            let mut cols = 0;
            for (i, v) in row.into_iter().enumerate() {
                let c = self.columns
                    .get(i)
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
                        self.nullmap.set(i + 2, true);
                    }
                } else {
                    v.to_mysql_bin(&mut self.data, c)?;
                }
                cols += 1;
            }

            if cols != self.columns.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "row has fewer columns than specification",
                ));
            }

            for (i, b) in self.nullmap.blocks().enumerate() {
                self.data[i] = b;
            }

            self.writer.write_all(&self.data[..])?;
            self.data.clear();
        } else {
            for v in row {
                v.to_mysql_text(self.writer)?;
            }
        }

        Ok(())
    }
}

impl<'a, W: Write + 'a> RowWriter<'a, W> {
    fn finish_inner(&mut self) -> io::Result<()> {
        self.finished = true;
        self.writer.end_packet()?;
        writers::write_eof_packet(self.writer, StatusFlags::empty())
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
