//! Bindings for emulating a MySQL/MariaDB server.
//!
//! When developing new databases or caching layers, it can be immensely useful to test your system
//! using existing applications. However, this often requires significant work modifying
//! applications to use your database over the existing ones. This crate solves that problem by
//! acting as a MySQL server, and delegating operations such as querying and query execution to
//! user-defined logic.
//!
//! To start, implement `MysqlShim` for your backend, and create a `MysqlIntermediary` over an
//! instance of your backend and a connection stream. The appropriate methods will be called on
//! your backend whenever a client issues a `QUERY`, `PREPARE`, or `EXECUTE` command, and you will
//! have a chance to respond appropriately. For example, to write a shim that always responds to
//! all commands with a "no results" reply:
//!
//! ```
//! # extern crate msql_srv;
//! extern crate mysql;
//! # use std::io;
//! # use std::net;
//! # use std::thread;
//! use msql_srv::*;
//!
//! struct Backend;
//! impl<W: io::Write> MysqlShim<W> for Backend {
//!     fn param_info(&self, stmt: u32) -> &[Column] { &[] }
//!
//!     fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
//!         info.reply(42, &[], &[])
//!     }
//!     fn on_execute(
//!         &mut self,
//!         _: u32,
//!         _: Vec<Value>,
//!         results: QueryResultWriter<W>,
//!     ) -> io::Result<()> {
//!         results.completed(0, 0)
//!     }
//!     fn on_close(&mut self, _: u32) {}
//!
//!     fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
//!         results.completed(0, 0)
//!     }
//! }
//!
//! fn main() {
//!     let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
//!     let port = listener.local_addr().unwrap().port();
//!
//!     let jh = thread::spawn(move || {
//!         if let Ok((s, _)) = listener.accept() {
//!             MysqlIntermediary::run_on_tcp(Backend, s).unwrap();
//!         }
//!     });
//!
//!     let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
//!     assert_eq!(db.ping(), true);
//!     assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 0);
//!     drop(db);
//!     jh.join().unwrap();
//! }
//! ```
#![feature(try_from)]
#![deny(missing_docs)]

extern crate byteorder;
extern crate chrono;
extern crate mysql_common as myc;
#[macro_use]
extern crate nom;

use std::io::prelude::*;
use std::net;
use std::io;
use std::iter;

pub use myc::constants::{ColumnFlags, ColumnType};
pub use myc::value::Value;

mod packet;
mod commands;
mod params;
mod value;
mod resultset;
mod writers;

/// Meta-information abot a single column, used either to describe a prepared statement parameter
/// or an output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// This column's associated table.
    ///
    /// Note that this is *technically* the table's alias.
    pub table: String,
    /// This column's name.
    ///
    /// Note that this is *technically* the column's alias.
    pub column: String,
    /// This column's type>
    pub coltype: ColumnType,
    /// Any flags associated with this column.
    ///
    /// Of particular interest are `ColumnFlags::UNSIGNED_FLAG` and `ColumnFlags::NOT_NULL_FLAG`.
    pub colflags: ColumnFlags,
}

pub use value::ToMysqlValue;
pub use resultset::{QueryResultWriter, RowWriter, StatementMetaWriter};

/// Implementors of this trait can be used to drive a MySQL-compatible database backend.
pub trait MysqlShim<W: Write> {
    /// Retrieve information about the parameters associated with a previously prepared statement.
    ///
    /// This is needed to correctly parse the parameters provided by the client when executing a
    /// prepared statement.
    fn param_info(&self, stmt: u32) -> &[Column];

    /// Called when the client issues a request to prepare `query` for later execution.
    ///
    /// The provided `info` handle should be used to notify the client of the statement id assigned
    /// to the prepared statement, as well as to give metadata about the types of parameters and
    /// returned columns.
    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()>;

    /// Called when the client executes a previously prepared statement.
    ///
    /// Any parameters included with the client's command is given in `params`.
    /// A response to the query should be given using the provided `QueryResultWriter`.
    fn on_execute(
        &mut self,
        id: u32,
        params: Vec<Value>,
        results: QueryResultWriter<W>,
    ) -> io::Result<()>;

    /// Called when the client wishes to deallocate resources associated with a previously prepared
    /// statement.
    fn on_close(&mut self, stmt: u32);

    /// Called when the client issues a query for immediate execution.
    ///
    /// Results should be returned using the given `QueryResultWriter`.
    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()>;
}

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements `MysqlShim`.
pub struct MysqlIntermediary<B, R: Read, W: Write> {
    shim: B,
    reader: packet::PacketReader<R>,
    writer: packet::PacketWriter<W>,
}

impl<B: MysqlShim<net::TcpStream>> MysqlIntermediary<B, net::TcpStream, net::TcpStream> {
    /// Create a new `MysqlIntermediary` over a TCP stream and process client commands until the
    /// client disconnects or an error occurs. See also `MysqlIntermediary::run_on`.
    pub fn run_on_tcp(shim: B, stream: net::TcpStream) -> io::Result<()> {
        let w = stream.try_clone()?;
        MysqlIntermediary::run_on(shim, stream, w)
    }
}

impl<B: MysqlShim<S>, S: Read + Write + Clone> MysqlIntermediary<B, S, S> {
    /// Create a new `MysqlIntermediary` over a two-way stream and process client commands until the
    /// client disconnects or an error occurs. See also `MysqlIntermediary::run_on`.
    pub fn run_on_stream(shim: B, stream: S) -> io::Result<()> {
        MysqlIntermediary::run_on(shim, stream.clone(), stream)
    }
}

impl<B: MysqlShim<W>, R: Read, W: Write> MysqlIntermediary<B, R, W> {
    /// Create a new `MysqlIntermediary` over two one-way channels and process client commands
    /// until the client disconnects or an error occurs.
    pub fn run_on(shim: B, reader: R, writer: W) -> io::Result<()> {
        let r = packet::PacketReader::new(reader);
        let w = packet::PacketWriter::new(writer);
        let mut mi = MysqlIntermediary {
            shim,
            reader: r,
            writer: w,
        };
        mi.init()?;
        mi.run()
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

    fn run(mut self) -> io::Result<()> {
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
                                            table: String::new(),
                                            column: "@@max_allowed_packet".to_owned(),
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

fn ok<W: Write>(w: &mut packet::PacketWriter<W>) -> io::Result<()> {
    w.write_all(&[0x00])?; // OK packet type
    w.write_all(&[0x00])?; // 0 rows (with lenenc encoding)
    w.write_all(&[0x00])?; // no inserted rows
    w.write_all(&[0x00, 0x00])?; // no server status
    w.write_all(&[0x00, 0x00])?; // no warnings
    w.flush()
}
