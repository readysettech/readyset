use std::io::{self, Write};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::myc::constants::StatusFlags;
use crate::myc::io::WriteMysqlExt;
use crate::packet::PacketConn;
use crate::{Column, ErrorKind};

/// Size of an EOF packet payload (without the 4-byte packet header)
pub(crate) const EOF_PACKET_LEN: usize = 5;

/// Total size of an EOF packet including the 4-byte header
pub(crate) const EOF_PACKET_TOTAL_LEN: usize = 4 + EOF_PACKET_LEN;

/// Write an EOF packet with its header into an existing buffer without allocating a new one.
/// The buffer must have at least EOF_PACKET_TOTAL_LEN bytes of remaining capacity.
pub(crate) fn write_eof_packet_inline<S>(
    buf: &mut Vec<u8>,
    conn: &mut PacketConn<S>,
    s: StatusFlags,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let hdr = conn.packet_header_bytes(EOF_PACKET_LEN);
    buf.write_all(&hdr)?;
    buf.extend([0xFE, 0x00, 0x00, s.bits() as u8, (s.bits() >> 8) as u8]);
    Ok(())
}

/// Write an EOF packet using its own buffer allocation.
/// Prefer `write_eof_packet_inline` when you already have a buffer to avoid extra allocation.
pub(crate) async fn write_eof_packet<S>(conn: &mut PacketConn<S>, s: StatusFlags) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = conn.get_buffer(EOF_PACKET_TOTAL_LEN);
    write_eof_packet_inline(&mut buf, conn, s)?;
    conn.enqueue_plain(buf);
    Ok(())
}

pub(crate) async fn write_ok_packet<S>(
    conn: &mut PacketConn<S>,
    rows: u64,
    last_insert_id: u64,
    s: StatusFlags,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    const MAX_OK_PACKET_LEN: usize = 1 + 9 + 9 + 2 + 2;
    let mut buf = conn.get_buffer(MAX_OK_PACKET_LEN);
    buf.write_u8(0x00)?; // OK packet type
    buf.write_lenenc_int(rows)?;
    buf.write_lenenc_int(last_insert_id)?;
    buf.write_u16::<LittleEndian>(s.bits())?;
    buf.write_all(&[0x00, 0x00])?; // no warnings
    conn.enqueue_packet(buf);
    Ok(())
}

pub async fn write_err<S>(err: ErrorKind, msg: &[u8], conn: &mut PacketConn<S>) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let size = 4 + 5 + msg.len();
    let mut buf = conn.get_buffer(size);
    buf.write_u8(0xFF)?;
    buf.write_u16::<LittleEndian>(err as u16)?;
    buf.write_u8(b'#')?;
    buf.write_all(err.sqlstate())?;
    buf.write_all(msg)?;
    conn.enqueue_packet(buf);
    Ok(())
}

pub(crate) async fn write_prepare_ok<S>(
    id: u32,
    params: &[Column],
    columns: &[Column],
    conn: &mut PacketConn<S>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    const MAX_PREPARE_OK_PACKET_LEN: usize = 1 + 4 + 2 + 2 + 1 + 2;

    // first, write out COM_STMT_PREPARE_OK
    let mut buf = conn.get_buffer(MAX_PREPARE_OK_PACKET_LEN);
    buf.write_u8(0x00)?;
    buf.write_u32::<LittleEndian>(id)?;
    buf.write_u16::<LittleEndian>(columns.len() as u16)?;
    buf.write_u16::<LittleEndian>(params.len() as u16)?;
    buf.write_u8(0x00)?;
    buf.write_u16::<LittleEndian>(0)?; // number of warnings
    conn.enqueue_packet(buf);

    write_column_definitions(params, conn, true).await?;
    write_column_definitions(columns, conn, true).await
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

// See https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html for documentation
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
    buf.write_u32::<LittleEndian>(c.column_length).unwrap();
    // Column Type (1 byte)
    buf.write_u8(c.coltype as u8).unwrap();
    // Column Flags (2 bytes)
    buf.write_u16::<LittleEndian>(c.colflags.bits()).unwrap();
    // Decimals (1 byte) - maximum shown decimal digits
    buf.write_all(&[c.decimals]).unwrap(); // decimals
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

pub(crate) async fn write_column_definitions<S>(
    columns: &[Column],
    conn: &mut PacketConn<S>,
    only_eof_on_nonempty: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if columns.is_empty() {
        return if only_eof_on_nonempty {
            Ok(())
        } else {
            write_eof_packet(conn, StatusFlags::empty()).await
        };
    }

    // Calculate total buffer size needed for all column definition packets + EOF packet
    // Each column needs: 4 bytes for packet header + encoded column definition
    // EOF packet needs: EOF_PACKET_TOTAL_LEN
    let columns_size: usize = columns.iter().map(|c| 4 + col_enc_len(c)).sum();
    let total_size = columns_size + EOF_PACKET_TOTAL_LEN;

    // Allocate a single buffer for all packets with their headers
    let mut buf = Vec::with_capacity(total_size);

    // Write all column definition packets (with headers) into the single buffer
    for c in columns {
        let col_len = col_enc_len(c);
        let hdr = conn.packet_header_bytes(col_len);
        buf.write_all(&hdr)?;
        write_column_definition(c, &mut buf);
    }

    // Write EOF packet inline into the same buffer
    write_eof_packet_inline(&mut buf, conn, StatusFlags::empty())?;

    // Enqueue the raw buffer containing all packets (column defs + EOF)
    conn.enqueue_plain(buf);
    Ok(())
}

pub(crate) async fn column_definitions<S>(
    columns: &[Column],
    conn: &mut PacketConn<S>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let size = lenc_int_len(columns.len() as u64);
    let mut buf = conn.get_buffer(size);
    buf.write_lenenc_int(columns.len() as u64)?;
    conn.enqueue_packet(buf);
    write_column_definitions(columns, conn, false).await
}

pub(crate) async fn column_definitions_cached<S>(
    columns: &[Column],
    cached: Arc<[u8]>,
    conn: &mut PacketConn<S>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Allocate a buffer for just the EOF packet to append after cached data
    let mut buf = conn.get_buffer(EOF_PACKET_TOTAL_LEN);

    // Enqueue the cached column definitions first
    conn.enqueue_raw(cached);
    conn.seq = conn.seq.wrapping_add((1 + columns.len()) as u8);

    // Write EOF packet inline into our buffer
    write_eof_packet_inline(&mut buf, conn, StatusFlags::empty())?;
    conn.enqueue_plain(buf);
    Ok(())
}
