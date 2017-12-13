#![feature(try_from)]

extern crate bit_vec;
extern crate byteorder;
extern crate chrono;
extern crate mysql_common as myc;
#[macro_use]
extern crate nom;

use std::io::prelude::*;
use std::net;
use std::io;
use std::iter;
use myc::constants::{ColumnFlags, ColumnType};

mod packet;
mod commands;
mod params;
mod value;
mod resultset;
mod writers;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub schema: String,
    pub table_alias: String,
    pub table: String,
    pub column_alias: String,
    pub column: String,
    pub coltype: ColumnType,
    pub colflags: ColumnFlags,
}

pub use value::ToMysqlValue;
pub use resultset::{QueryResultWriter, RowWriter};

#[must_use]
pub struct StatementMetaWriter<'a, W: Write + 'a> {
    writer: &'a mut packet::PacketWriter<W>,
}

impl<'a, W: Write + 'a> StatementMetaWriter<'a, W> {
    pub fn write<PI, CI>(self, id: u32, params: PI, columns: CI) -> io::Result<()>
    where
        PI: IntoIterator<Item = &'a Column>,
        CI: IntoIterator<Item = &'a Column>,
        <PI as IntoIterator>::IntoIter: ExactSizeIterator,
        <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        writers::write_prepare_ok(id, params, columns, self.writer)
    }
}

pub trait MysqlShim<W: Write> {
    fn param_info(&self, stmt: u32) -> &[Column];

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()>;
    fn on_execute(
        &mut self,
        id: u32,
        params: Vec<myc::value::Value>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;
    fn on_close(&mut self, stmt: u32);

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()>;
}

pub struct MysqlIntermediary<B, R: Read, W: Write> {
    shim: B,
    reader: packet::PacketReader<R>,
    writer: packet::PacketWriter<W>,
}

impl<B> MysqlIntermediary<B, net::TcpStream, net::TcpStream> {
    pub fn from_stream(shim: B, stream: net::TcpStream) -> io::Result<Self> {
        let w = stream.try_clone()?;
        MysqlIntermediary::new(shim, stream, w)
    }
}

impl<B, S: Read + Write + Clone> MysqlIntermediary<B, S, S> {
    pub fn from_cloneable(shim: B, stream: S) -> io::Result<Self> {
        MysqlIntermediary::new(shim, stream.clone(), stream)
    }
}

impl<B, R: Read, W: Write> MysqlIntermediary<B, R, W> {
    pub fn new(shim: B, reader: R, writer: W) -> io::Result<Self> {
        let r = packet::PacketReader::new(reader);
        let w = packet::PacketWriter::new(writer);
        let mut mi = MysqlIntermediary {
            shim,
            reader: r,
            writer: w,
        };
        mi.init()?;
        Ok(mi)
    }

    fn init(&mut self) -> io::Result<()> {
        self.writer.write_all(&[10])?; // protocol 10
        self.writer.write_all(&b"0.0.1-alpha-msql-proxy\0"[..])?;
        self.writer.write_all(&[0x08, 0x00, 0x00, 0x00])?; // connection ID
        self.writer.write_all(&b";X,po_k}\0"[..])?; // auth seed
        self.writer.write_all(&[0x00, 0x42])?; // just 4.1 proto
        self.writer.write_all(&[0x21])?; // UTF8_GENERAL_CI
        self.writer.write_all(&[0x00, 0x00])?; // status flags
        self.writer.write_all(&[0x00, 0x00])?; // extended capabilities
        self.writer.write_all(&[0x00])?; // no plugins
        self.writer.write_all(&[0x00; 6][..])?; // filler
        self.writer.write_all(&[0x00; 4][..])?; // filler
        self.writer.write_all(&b">o6^Wz!/kM}N\0"[..])?; // 4.1+ servers must extend salt
        self.writer.flush()?;

        {
            let (seq, handshake) = self.reader.next()?.unwrap();
            let _handshake = commands::client_handshake(&handshake).unwrap().1;
            self.writer.set_seq(seq + 1);
        }

        ok(&mut self.writer)
    }
}

fn ok<W: Write>(w: &mut packet::PacketWriter<W>) -> io::Result<()> {
    w.write_all(&[0x00])?; // OK packet type
    w.write_all(&[0x00])?; // 0 rows (with lenenc encoding)
    w.write_all(&[0x00])?; // no inserted rows
    w.write_all(&[0x00, 0x00])?; // no server status
    w.write_all(&[0x00, 0x00])?; // no warnings
    w.flush()
}

impl<B: MysqlShim<W>, R: Read, W: Write> MysqlIntermediary<B, R, W> {
    pub fn run(mut self) -> io::Result<()> {
        use commands::Command;

        while let Some((seq, packet)) = self.reader.next()? {
            self.writer.set_seq(seq + 1);
            let cmd = commands::parse(&packet).unwrap().1;
            match cmd {
                Command::Query(q) => {
                    let skip = {
                        let w = QueryResultWriter {
                            is_bin: false,
                            writer: &mut self.writer,
                        };

                        if q.starts_with(b"SELECT @@") {
                            let var = &q[b"SELECT @@".len()..];
                            match var {
                                b"max_allowed_packet" => {
                                    let cols = &[
                                        Column {
                                            schema: String::new(),
                                            table_alias: String::new(),
                                            table: String::new(),
                                            column_alias: "@@max_allowed_packet".to_owned(),
                                            column: String::new(),
                                            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                                            colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                                        },
                                    ];
                                    let mut w = w.start(cols)?;
                                    w.write_row(iter::once(1024u16))?;
                                    w.finish()?;
                                }
                                _ => {
                                    w.completed(0, 0)?;
                                }
                            }
                            true
                        } else {
                            self.shim.on_query(
                                ::std::str::from_utf8(q)
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                                w,
                            )?;
                            false
                        }
                    };

                    self.writer.flush()?;

                    if skip {
                        continue;
                    }
                }
                Command::Prepare(q) => {
                    {
                        let w = StatementMetaWriter {
                            writer: &mut self.writer,
                        };

                        self.shim.on_prepare(
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )?;
                    }

                    self.writer.flush()?;
                }
                Command::Execute { stmt, params } => {
                    let params = params::parse(params, self.shim.param_info(stmt))
                        .map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e))
                        })?
                        .1;

                    {
                        let w = QueryResultWriter {
                            is_bin: true,
                            writer: &mut self.writer,
                        };

                        self.shim.on_execute(stmt, params, w)?;
                    }
                    self.writer.flush()?;
                }
                Command::Close(stmt) => {
                    self.shim.on_close(stmt);
                    ok(&mut self.writer)?;
                }
                Command::Init(_) | Command::Ping => {
                    ok(&mut self.writer)?;
                }
                Command::Quit => {
                    break;
                }
            }
        }
        Ok(())
    }
}
