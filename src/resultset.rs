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

            for (i, v) in row.into_iter().enumerate() {
                let c = self.columns[i].borrow();
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

#[must_use]
pub struct QueryResultWriter<'a, W: Write + 'a> {
    // XXX: specialization instead?
    pub(crate) is_bin: bool,
    pub(crate) writer: &'a mut PacketWriter<W>,
}

impl<'a, W: Write> QueryResultWriter<'a, W> {
    pub fn start(self, columns: &'a [Column]) -> io::Result<RowWriter<'a, W>> {
        RowWriter::new(self.writer, columns, self.is_bin)
    }

    pub fn completed(self, rows: u64, last_insert_id: u64) -> io::Result<()> {
        self.writer.write_u8(0x00)?; // OK packet type
        self.writer.write_lenenc_int(rows)?;
        self.writer.write_lenenc_int(last_insert_id)?;
        self.writer.write_all(&[0x00, 0x00])?; // no server status
        self.writer.write_all(&[0x00, 0x00])?; // no warnings
        Ok(())
    }
}
