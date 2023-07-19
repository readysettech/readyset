use std::io::{self, Write};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use tokio::io::AsyncWrite;

use crate::myc::constants::StatusFlags;
use crate::myc::io::WriteMysqlExt;
use crate::packet::PacketWriter;
use crate::{Column, ErrorKind};

pub(crate) async fn write_eof_packet<W: AsyncWrite + Unpin>(
    w: &mut PacketWriter<W>,
    s: StatusFlags,
) -> io::Result<()> {
    let mut buf = w.get_buffer();
    buf.extend([0xFE, 0x00, 0x00, s.bits() as u8, (s.bits() >> 8) as u8]);
    w.enqueue_packet(buf);
    Ok(())
}

pub(crate) async fn write_ok_packet<W: AsyncWrite + Unpin>(
    w: &mut PacketWriter<W>,
    rows: u64,
    last_insert_id: u64,
    s: StatusFlags,
) -> io::Result<()> {
    const MAX_OK_PACKET_LEN: usize = 1 + 9 + 9 + 2 + 2;
    let mut buf = w.get_buffer();
    buf.reserve(MAX_OK_PACKET_LEN);
    buf.write_u8(0x00)?; // OK packet type
    buf.write_lenenc_int(rows)?;
    buf.write_lenenc_int(last_insert_id)?;
    buf.write_u16::<LittleEndian>(s.bits())?;
    buf.write_all(&[0x00, 0x00])?; // no warnings
    w.enqueue_packet(buf);
    Ok(())
}

pub async fn write_err<W: AsyncWrite + Unpin>(
    err: ErrorKind,
    msg: &[u8],
    w: &mut PacketWriter<W>,
) -> io::Result<()> {
    let mut buf = w.get_buffer();
    buf.reserve(4 + 5 + msg.len());
    buf.write_u8(0xFF)?;
    buf.write_u16::<LittleEndian>(err as u16)?;
    buf.write_u8(b'#')?;
    buf.write_all(err.sqlstate())?;
    buf.write_all(msg)?;
    w.write_packet(&buf).await
}

pub(crate) async fn write_prepare_ok<'a, PI, CI, W>(
    id: u32,
    params: PI,
    columns: CI,
    w: &mut PacketWriter<W>,
) -> io::Result<()>
where
    PI: IntoIterator<Item = &'a Column>,
    CI: IntoIterator<Item = &'a Column>,
    <PI as IntoIterator>::IntoIter: ExactSizeIterator,
    <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    W: AsyncWrite + Unpin,
{
    const MAX_PREPARE_OK_PACKET_LEN: usize = 1 + 4 + 2 + 2 + 1 + 2;

    let pi = params.into_iter();
    let ci = columns.into_iter();

    // first, write out COM_STMT_PREPARE_OK
    let mut buf = w.get_buffer();
    buf.reserve(MAX_PREPARE_OK_PACKET_LEN);
    buf.write_u8(0x00)?;
    buf.write_u32::<LittleEndian>(id)?;
    buf.write_u16::<LittleEndian>(ci.len() as u16)?;
    buf.write_u16::<LittleEndian>(pi.len() as u16)?;
    buf.write_u8(0x00)?;
    buf.write_u16::<LittleEndian>(0)?; // number of warnings
    w.enqueue_packet(buf);

    write_column_definitions(pi, w, true).await?;
    write_column_definitions(ci, w, true).await
}

/// Compute the size of the buffer required to encode this buffer
const fn lenc_str_len(s: &[u8]) -> usize {
    s.len() + lenc_int_len(s.len() as _)
}

const fn lenc_int_len(n: u64) -> usize {
    match n {
        0..=250 => 1,
        251..=65_535 => 3,
        65_536..=16_777_215 => 4,
        _ => 9,
    }
}

/// Compute the size of the buffer required to encode this column definition
fn col_enc_len(c: &Column) -> usize {
    lenc_str_len(b"def")
        + lenc_str_len(b"")
        + lenc_str_len(c.table.as_bytes())
        + lenc_str_len(b"")
        + lenc_str_len(c.column.as_bytes())
        + lenc_str_len(b"")
        + (1 + 2 + 4 + 1 + 2 + 1 + 2)
}

// See https://dev.mysql.com/doc/internals/en/com-query-response.html for documentation
fn write_column_definition(c: &Column, buf: &mut Vec<u8>) {
    // The following unwraps are fine because writes to a Vec can't fail

    // Catalog (lenenc)
    buf.write_lenenc_str(b"def").unwrap();
    // Schema (lenenc)
    buf.write_lenenc_str(b"").unwrap();
    // Table (lenenc)
    buf.write_lenenc_str(c.table.as_bytes()).unwrap();
    // Original Table (lenenc)
    buf.write_lenenc_str(b"").unwrap();
    // Name (lenenc)
    buf.write_lenenc_str(c.column.as_bytes()).unwrap();
    // Original Name (lenenc)
    buf.write_lenenc_str(b"").unwrap();
    // Next Length (lenenc) - always 0x0c
    buf.write_lenenc_int(0x0C).unwrap();
    // Character Set (2 Bytes)
    buf.write_u16::<LittleEndian>(c.character_set).unwrap();
    // Column Length (4 bytes) - maximum display length
    //
    // TODO: `column_length` should not be an option. Using 1024 as a default is not necessarily
    // correct
    buf.write_u32::<LittleEndian>(c.column_length.unwrap_or(1024))
        .unwrap();
    // Column Type (1 byte)
    buf.write_u8(c.coltype as u8).unwrap();
    // Column Flags (2 bytes)
    buf.write_u16::<LittleEndian>(c.colflags.bits()).unwrap();
    // Decimals (1 byte) - maximum shown decimal digits
    buf.write_all(&[0x00]).unwrap(); // decimals
    buf.write_all(&[0x00, 0x00]).unwrap(); // unused
}

/// Preencode the column definitions into a buffer for future reuse
pub fn prepare_column_definitions(cols: &[Column]) -> Vec<u8> {
    let total_len: usize = cols.iter().map(|c| col_enc_len(c) + 4).sum();
    let mut buf = Vec::with_capacity(total_len + 9);

    let hdr = lenc_int_len(cols.len() as u64) as u32 | (1u32 << 24);
    buf.write_u32::<LittleEndian>(hdr).unwrap();
    buf.write_lenenc_int(cols.len() as u64).unwrap();

    for (seq, c) in cols.iter().enumerate() {
        let hdr = col_enc_len(c) as u32 | (((seq + 2) as u32) << 24);
        buf.write_u32::<LittleEndian>(hdr).unwrap();
        write_column_definition(c, &mut buf);
    }
    buf
}

pub(crate) async fn write_column_definitions<'a, I, W>(
    i: I,
    w: &mut PacketWriter<W>,
    only_eof_on_nonempty: bool,
) -> io::Result<()>
where
    I: IntoIterator<Item = &'a Column>,
    W: AsyncWrite + Unpin,
{
    let mut empty = true;
    for c in i {
        let mut buf = w.get_buffer();
        buf.reserve(col_enc_len(c));
        write_column_definition(c, &mut buf);
        w.enqueue_packet(buf);
        empty = false;
    }

    if empty && only_eof_on_nonempty {
        Ok(())
    } else {
        write_eof_packet(w, StatusFlags::empty()).await
    }
}

pub(crate) async fn column_definitions<'a, I, W>(i: I, w: &mut PacketWriter<W>) -> io::Result<()>
where
    I: IntoIterator<Item = &'a Column>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
    W: AsyncWrite + Unpin,
{
    let i = i.into_iter();
    let mut buf = w.get_buffer();
    buf.write_lenenc_int(i.len() as u64)?;
    w.enqueue_packet(buf);
    write_column_definitions(i, w, false).await
}

pub(crate) async fn column_definitions_cached<'a, I, W>(
    i: I,
    cached: Arc<[u8]>,
    w: &mut PacketWriter<W>,
) -> io::Result<()>
where
    I: IntoIterator<Item = &'a Column>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
    W: AsyncWrite + Unpin,
{
    let i = i.into_iter();
    w.enqueue_raw(cached).await?;
    w.seq = w.seq.wrapping_add((1 + i.len()) as u8);
    write_eof_packet(w, StatusFlags::empty()).await
}
