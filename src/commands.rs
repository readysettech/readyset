use nom;
use myc;
use myc::constants::{CapabilityFlags, ColumnFlags, ColumnType, Command as CommandByte, StatusFlags};
use myc::io::WriteMysqlExt;
use std::io::{self, Write};
use byteorder::{LittleEndian, WriteBytesExt};
use bit_vec::BitVec;

use packet::PacketWriter;

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
    Close(u32),
    Prepare(&'a [u8]),
    Init(&'a [u8]),
    Execute { stmt: u32, params: &'a [u8] },
    Ping,
    Quit,
}

named!(
    execute<Command>,
    do_parse!(
        stmt: apply!(nom::le_u32,) >>
        _flags: take!(1) >>
        _iterations: apply!(nom::le_u32,) >>
        rest: apply!(nom::rest,) >> //rustfmt
        (Command::Execute {
            stmt,
            params: rest,
        })
    )
);

named!(
    pub command<Command>,
    alt!(
        preceded!(tag!(&[CommandByte::COM_QUERY as u8]), apply!(nom::rest,)) => { |sql| Command::Query(sql) } |
        preceded!(tag!(&[CommandByte::COM_INIT_DB as u8]), apply!(nom::rest,)) => { |db| Command::Init(db) } |
        preceded!(tag!(&[CommandByte::COM_STMT_PREPARE as u8]), apply!(nom::rest,)) => { |sql| Command::Prepare(sql) } |
        preceded!(tag!(&[CommandByte::COM_STMT_EXECUTE as u8]), execute) |
        preceded!(tag!(&[CommandByte::COM_STMT_CLOSE as u8]), apply!(nom::le_u32,)) => { |stmt| Command::Close(stmt)} |
        tag!(&[CommandByte::COM_QUIT as u8]) => { |_| Command::Quit } |
        tag!(&[CommandByte::COM_PING as u8]) => { |_| Command::Ping }
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

fn write_eof_packet<W: Write>(w: &mut PacketWriter<W>, s: StatusFlags) -> io::Result<()> {
    w.write_all(&[0xFE, 0x00, 0x00])?;
    w.write_u16::<LittleEndian>(s.bits())?;
    w.end_packet()
}

use std::borrow::Borrow;

pub fn write_prepare_ok<'a, PI, PE, CI, CE, W>(
    id: u32,
    params: PI,
    columns: CI,
    w: &mut PacketWriter<W>,
) -> io::Result<()>
where
    PI: IntoIterator<Item = PE>,
    CI: IntoIterator<Item = CE>,
    PE: Borrow<Column<'a>>,
    CE: Borrow<Column<'a>>,
    <PI as IntoIterator>::IntoIter: ExactSizeIterator,
    <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    W: Write,
{
    let pi = params.into_iter();
    let ci = columns.into_iter();

    // first, write out COM_STMT_PREPARE_OK
    w.write_u8(0x00)?;
    w.write_u32::<LittleEndian>(id)?;
    w.write_u16::<LittleEndian>(ci.len() as u16)?;
    w.write_u16::<LittleEndian>(pi.len() as u16)?;
    w.write_u8(0x00)?;
    w.write_u16::<LittleEndian>(0)?; // number of warnings
    w.end_packet()?;

    write_column_definitions(pi, w)?;
    write_column_definitions(ci, w)
}

fn write_column_definitions<'a, I, E, W>(i: I, w: &mut PacketWriter<W>) -> io::Result<()>
where
    I: IntoIterator<Item = E>,
    E: Borrow<Column<'a>>,
    W: Write,
{
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
        w.end_packet()?;
    }
    write_eof_packet(w, StatusFlags::empty())
}

pub fn column_definitions<'a, I, E, W>(i: I, w: &mut PacketWriter<W>) -> io::Result<()>
where
    I: IntoIterator<Item = E>,
    E: Borrow<Column<'a>>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
    W: Write,
{
    let i = i.into_iter();
    w.write_lenenc_int(i.len() as u64)?;
    w.end_packet()?;
    write_column_definitions(i, w)
}

pub fn write_resultset_rows_text<'a, O, I, E, W>(rows: O, w: &mut PacketWriter<W>) -> io::Result<()>
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

fn write_bin_value(data: &mut Vec<u8>, v: &myc::value::Value, c: &Column) -> io::Result<()> {
    // unfortunately only *some* of the types have the same encoding server->client as
    // client->server. compare
    // https://docs.rs/mysql_common/0.4.0/src/mysql_common/io.rs.html#29-131 and
    // https://mariadb.com/kb/en/library/resultset-row/#tinyint-binary-encoding
    let signed = c.colflags.contains(ColumnFlags::UNSIGNED_FLAG);
    let ct = c.coltype;
    match ct {
        ColumnType::MYSQL_TYPE_DOUBLE
        | ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_SET
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_DECIMAL
        | ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_NEWDECIMAL
        | ColumnType::MYSQL_TYPE_GEOMETRY
        | ColumnType::MYSQL_TYPE_JSON => {
            // NOTE: we should sort of check the underlying type here too...
            data.write_bin_value(v)?;
            return Ok(());
        }
        _ => {}
    }

    // myc uses Value::Int for everything except unsigned 64-bit integers (LONGLONG)
    match (v, signed) {
        (&myc::value::Value::Int(v), true) => match ct {
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                return data.write_i32::<LittleEndian>(v as i32);
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                return data.write_i16::<LittleEndian>(v as i16);
            }
            ColumnType::MYSQL_TYPE_TINY => {
                return data.write_i8(v as i8);
            }
            _ => {}
        },
        (&myc::value::Value::Int(v), false) => match ct {
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                return data.write_u32::<LittleEndian>(v as u32);
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                return data.write_u16::<LittleEndian>(v as u16);
            }
            ColumnType::MYSQL_TYPE_TINY => {
                return data.write_u8(v as u8);
            }
            _ => {}
        },
        _ => {}
    }

    match (v, ct) {
        (&myc::value::Value::Float(f), ColumnType::MYSQL_TYPE_FLOAT) => {
            return data.write_f32::<LittleEndian>(f as f32);
        }
        (&myc::value::Value::Time(neg, d, h, m, s, u), ColumnType::MYSQL_TYPE_TIME) => {
            if u == 0 {
                data.write_u8(8u8)?;
            } else {
                data.write_u8(12u8)?;
            }
            data.write_u8(if neg { 1u8 } else { 0u8 })?;
            data.write_u32::<LittleEndian>(d)?;
            data.write_u8(h)?;
            data.write_u8(m)?;
            data.write_u8(s)?;
            if u != 0 {
                data.write_u32::<LittleEndian>(u)?;
            }
            return Ok(());
        }
        (&myc::value::Value::Date(y, m, d, h, i, s, u), _) => match ct {
            ColumnType::MYSQL_TYPE_DATE => {
                data.write_u8(4u8)?;
                data.write_u16::<LittleEndian>(y)?;
                data.write_u8(m)?;
                data.write_u8(d)?;
                return Ok(());
            }
            ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => {
                if u == 0 {
                    data.write_u8(7u8)?;
                } else {
                    data.write_u8(11u8)?;
                }
                data.write_u16::<LittleEndian>(y)?;
                data.write_u8(m)?;
                data.write_u8(d)?;
                data.write_u8(h)?;
                data.write_u8(i)?;
                data.write_u8(s)?;
                if u != 0 {
                    data.write_u32::<LittleEndian>(u)?;
                }
                return Ok(());
            }
            _ => {}
        },
        _ => {}
    }

    eprintln!("tried to use {:?} as {:?}", v, ct);
    unreachable!();
}

pub fn write_resultset_rows_bin<'a, O, I, E, W, C>(
    rows: O,
    columns: &[C],
    w: &mut PacketWriter<W>,
) -> io::Result<()>
where
    O: IntoIterator<Item = I>,
    I: IntoIterator<Item = E>,
    E: Borrow<myc::value::Value>,
    C: Borrow<Column<'a>>,
    W: Write,
{
    let mut data = Vec::new();
    let mut nullmap = BitVec::<u8>::default();
    nullmap.grow(columns.len() + 9, false);
    let bitmap_len = nullmap.blocks().count();

    for row in rows {
        //w.write_u8(0x00)?;
        nullmap.clear();
        data.clear();

        // leave space for nullmap
        data.resize(bitmap_len, 0);

        let mut coli = 0;
        for col in row {
            match *col.borrow() {
                myc::value::Value::NULL => {
                    nullmap.set(coli + 2, true);
                }
                ref v => {
                    write_bin_value(&mut data, v, columns[coli].borrow()).unwrap();
                }
            }
            coli += 1;
        }
        assert_eq!(coli, columns.len());

        for (i, b) in nullmap.blocks().enumerate() {
            data[i] = b;
        }
        w.write_all(&data[..])?;
    }
    w.end_packet()?;
    write_eof_packet(w, StatusFlags::empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use myc::constants::{CapabilityFlags, UTF8_GENERAL_CI};
    use PacketReader;
    use std::io::Cursor;

    #[test]
    fn it_parses_handshake() {
        let data = &[
            0x25, 0x00, 0x00, 0x01, 0x85, 0xa6, 0x3f, 0x20, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x6f, 0x6e, 0x00, 0x00,
        ];
        let r = Cursor::new(&data[..]);
        let mut pr = PacketReader::new(r);
        let (_, p) = pr.next().unwrap().unwrap();
        let (_, handshake) = client_handshake(&p).unwrap();
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
        let r = Cursor::new(&data[..]);
        let mut pr = PacketReader::new(r);
        let (_, p) = pr.next().unwrap().unwrap();
        let (_, cmd) = command(&p).unwrap();
        assert_eq!(
            cmd,
            Command::Query(&b"select @@version_comment limit 1"[..])
        );
    }
}
