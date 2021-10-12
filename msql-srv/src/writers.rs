use crate::myc::constants::StatusFlags;
use crate::myc::io::WriteMysqlExt;
use crate::packet::PacketWriter;
use crate::{Column, ErrorKind};
use byteorder::{LittleEndian, WriteBytesExt};
use std::borrow::Borrow;
use std::io::{self, Cursor, Write};
use tokio::io::AsyncWrite;

pub(crate) async fn write_eof_packet<W: AsyncWrite + Unpin>(
    w: &mut PacketWriter<W>,
    s: StatusFlags,
) -> io::Result<()> {
    let buf: [u8; 5] = [0xFE, 0x00, 0x00, s.bits() as u8, (s.bits() >> 8) as u8];
    w.write_packet(&buf).await
}

pub(crate) async fn write_ok_packet<W: AsyncWrite + Unpin>(
    w: &mut PacketWriter<W>,
    rows: u64,
    last_insert_id: u64,
    s: StatusFlags,
) -> io::Result<()> {
    const MAX_OK_PACKET_LEN: usize = 1 + 9 + 9 + 2 + 2;
    let mut inner_buf = [0u8; MAX_OK_PACKET_LEN];
    let mut buf = Cursor::new(&mut inner_buf[..]);
    buf.write_u8(0x00)?; // OK packet type
    buf.write_lenenc_int(rows)?;
    buf.write_lenenc_int(last_insert_id)?;
    buf.write_u16::<LittleEndian>(s.bits())?;
    buf.write_all(&[0x00, 0x00])?; // no warnings
    w.write_packet(&buf.get_ref()[..buf.position() as usize])
        .await
}

pub async fn write_err<W: AsyncWrite + Unpin>(
    err: ErrorKind,
    msg: &[u8],
    w: &mut PacketWriter<W>,
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(4 + 5 + msg.len());
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
    let mut inner_buf = [0u8; MAX_PREPARE_OK_PACKET_LEN];
    let mut buf = Cursor::new(&mut inner_buf[..]);

    let pi = params.into_iter();
    let ci = columns.into_iter();

    // first, write out COM_STMT_PREPARE_OK
    buf.write_u8(0x00)?;
    buf.write_u32::<LittleEndian>(id)?;
    buf.write_u16::<LittleEndian>(ci.len() as u16)?;
    buf.write_u16::<LittleEndian>(pi.len() as u16)?;
    buf.write_u8(0x00)?;
    buf.write_u16::<LittleEndian>(0)?; // number of warnings
    w.write_packet(&buf.get_ref()[..buf.position() as usize])
        .await?;

    write_column_definitions(pi, w, true).await?;
    write_column_definitions(ci, w, true).await
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
        let mut buf = Vec::with_capacity(
            4 + 1 + 9 + c.table.len() + 1 + 9 + c.column.len() + 1 + 1 + 2 + 4 + 1 + 2 + 1 + 2,
        );

        let c = c.borrow();
        use crate::myc::constants::UTF8_GENERAL_CI;
        buf.write_lenenc_str(b"def")?;
        buf.write_lenenc_str(b"")?;
        buf.write_lenenc_str(c.table.as_bytes())?;
        buf.write_lenenc_str(b"")?;
        buf.write_lenenc_str(c.column.as_bytes())?;
        buf.write_lenenc_str(b"")?;
        buf.write_lenenc_int(0xC)?;
        buf.write_u16::<LittleEndian>(UTF8_GENERAL_CI)?;
        buf.write_u32::<LittleEndian>(1024)?;
        buf.write_u8(c.coltype as u8)?;
        buf.write_u16::<LittleEndian>(c.colflags.bits())?;
        buf.write_all(&[0x00])?; // decimals
        buf.write_all(&[0x00, 0x00])?; // unused

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
    let mut buf = Vec::new();
    let i = i.into_iter();
    buf.write_lenenc_int(i.len() as u64)?;
    w.write_packet(&buf).await?;
    write_column_definitions(i, w, false).await
}
