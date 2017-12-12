extern crate byteorder;
extern crate mysql_common as myc;
#[macro_use]
extern crate nom;

use myc::constants::{CapabilityFlags, ColumnFlags, ColumnType, Command as CommandByte, StatusFlags};
use myc::io::WriteMysqlExt;
use std::io::{self, Write};
use byteorder::{LittleEndian, WriteBytesExt};

mod packet;

pub use packet::{PacketReader, PacketWriter};

#[derive(Debug)]
pub struct ClientHandshake<'a> {
    capabilities: CapabilityFlags,
    maxps: u32,
    collation: u16,
    username: &'a [u8],
}

named!(
    pub client_handshake<ClientHandshake>,
    do_parse!(
        cap: apply!(nom::le_u32,) >>
        maxps: apply!(nom::le_u32,) >>
        collation: take!(1) >>
        take!(23) >>
        username: take_until!(&b"\0"[..]) >>
        tag!(b"\0") >> //rustfmt
        (ClientHandshake {
            capabilities: CapabilityFlags::from_bits_truncate(cap),
            maxps,
            collation: u16::from(collation[0]),
            username,
        })
    )
);

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Query(&'a [u8]),
    Init(&'a [u8]),
    Ping,
    Quit,
}

named!(
    pub command<Command>,
    alt!(
        preceded!(tag!(&[CommandByte::COM_QUERY as u8]), apply!(nom::rest,)) => { |sql| Command::Query(sql) } |
        preceded!(tag!(&[CommandByte::COM_INIT_DB as u8]), apply!(nom::rest,)) => { |db| Command::Init(db) } |
        tag!(&[CommandByte::COM_QUIT as u8]) => { |_| Command::Quit } |
        tag!(&[CommandByte::COM_PING as u8]) => { |_| Command::Ping }
    )
);

named!(
    pub lenencint<Result<Option<i64>, ()>>,
    alt!(
        tag!(&[0xFB]) => { |_| Ok(None) } |
        preceded!(tag!(&[0xFC]), apply!(nom::le_i16,)) => { |i| Ok(Some(i64::from(i))) } |
        preceded!(tag!(&[0xFD]), apply!(nom::le_i24,)) => { |i| Ok(Some(i64::from(i))) } |
        preceded!(tag!(&[0xFE]), apply!(nom::le_i64,)) => { |i| Ok(Some(i)) } |
        tag!(&[0xFF]) => { |_| Err(()) } |
        take!(1) => { |i: &[u8]| Ok(Some(i64::from(i[0]))) }
    )
);

pub struct Column<'a> {
    pub schema: &'a str,
    pub table_alias: &'a str,
    pub table: &'a str,
    pub column_alias: &'a str,
    pub column: &'a str,
    pub coltype: ColumnType,
    pub colflags: ColumnFlags,
}

pub fn write_eof_packet<W: Write>(w: &mut PacketWriter<W>, s: StatusFlags) -> io::Result<()> {
    w.write_all(&[0xFE, 0x00, 0x00])?;
    w.write_u16::<LittleEndian>(s.bits())?;
    w.end_packet()
}

use std::borrow::Borrow;
pub fn start_resultset_text<'a, I, E, W>(i: I, w: &mut PacketWriter<W>) -> io::Result<()>
where
    I: IntoIterator<Item = E>,
    E: Borrow<Column<'a>>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
    W: Write,
{
    let i = i.into_iter();
    w.write_lenenc_int(i.len() as u64)?;
    w.end_packet()?;
    for c in i {
        let c = c.borrow();
        use myc::constants::UTF8_GENERAL_CI;
        w.write_lenenc_str(b"def")?;
        w.write_lenenc_str(c.schema.as_bytes())?;
        w.write_lenenc_str(c.table_alias.as_bytes())?;
        w.write_lenenc_str(c.table.as_bytes())?;
        w.write_lenenc_str(c.column_alias.as_bytes())?;
        w.write_lenenc_str(c.column.as_bytes())?;
        w.write_lenenc_int(0xC)?;
        w.write_u16::<LittleEndian>(UTF8_GENERAL_CI)?;
        w.write_u32::<LittleEndian>(1024)?;
        w.write_u8(c.coltype as u8)?;
        w.write_u16::<LittleEndian>(c.colflags.bits())?;
        w.write_all(&[0x00])?;
    }
    w.end_packet()?;
    write_eof_packet(w, StatusFlags::empty())
}

pub fn write_resultset_text<'a, O, I, E, W>(rows: O, w: &mut PacketWriter<W>) -> io::Result<()>
where
    O: IntoIterator<Item = I>,
    I: IntoIterator<Item = E>,
    E: Borrow<myc::value::Value>,
    W: Write,
{
    for row in rows {
        for col in row {
            let col = col.borrow();
            w.write_lenenc_str(col.as_sql(true).as_bytes())?;
        }
    }
    w.end_packet()?;
    write_eof_packet(w, StatusFlags::empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql::consts::{CapabilityFlags, UTF8_GENERAL_CI};

    #[test]
    fn it_parses_handshake() {
        let data = &[
            0x25, 0x00, 0x00, 0x01, 0x85, 0xa6, 0x3f, 0x20, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x6f, 0x6e, 0x00, 0x00,
        ];
        let (_, p) = packet(&data[..]).unwrap();
        let (_, handshake) = client_handshake(&p.1).unwrap();
        println!("{:?}", handshake);
        assert!(
            handshake
                .capabilities
                .contains(CapabilityFlags::CLIENT_LONG_PASSWORD)
        );
        assert!(
            handshake
                .capabilities
                .contains(CapabilityFlags::CLIENT_MULTI_RESULTS)
        );
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB));
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF));
        assert_eq!(handshake.collation, UTF8_GENERAL_CI);
        assert_eq!(handshake.username, &b"jon"[..]);
        assert_eq!(handshake.maxps, 16777216);
    }

    #[test]
    fn it_parses_request() {
        let data = &[
            0x21, 0x00, 0x00, 0x00, 0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e,
            0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
        ];
        let (_, p) = packet(&data[..]).unwrap();
        let (_, cmd) = command(&p.1).unwrap();
        assert_eq!(
            cmd,
            Command::Query(&b"select @@version_comment limit 1"[..])
        );
    }
}
