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
//!     type Error = io::Error;
//!
//!     fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
//!         info.reply(42, &[], &[])
//!     }
//!     fn on_execute(
//!         &mut self,
//!         _: u32,
//!         _: ParamParser,
//!         results: QueryResultWriter<W>,
//!     ) -> io::Result<()> {
//!         results.completed(0, 0)
//!     }
//!     fn on_close(&mut self, _: u32) {}
//!
//!     fn on_init(&mut self, _: &str, writer: InitWriter<W>) -> io::Result<()> { Ok(()) }
//!
//!     fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
//!         let cols = [
//!             Column {
//!                 table: "foo".to_string(),
//!                 column: "a".to_string(),
//!                 coltype: ColumnType::MYSQL_TYPE_LONGLONG,
//!                 colflags: ColumnFlags::empty(),
//!             },
//!             Column {
//!                 table: "foo".to_string(),
//!                 column: "b".to_string(),
//!                 coltype: ColumnType::MYSQL_TYPE_STRING,
//!                 colflags: ColumnFlags::empty(),
//!             },
//!         ];
//!
//!         let mut rw = results.start(&cols)?;
//!         rw.write_col(42)?;
//!         rw.write_col("b's value")?;
//!         rw.finish()
//!     }
//!
//!     fn password_for_username(&self, _username: &[u8]) -> Option<Vec<u8>> {
//!         Some(b"password".to_vec())
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
//!     let mut db =
//!         mysql::Conn::new(&format!("mysql://root:password@127.0.0.1:{}", port)).unwrap();
//!     assert_eq!(db.ping(), true);
//!     assert_eq!(db.query("SELECT a, b FROM foo").unwrap().count(), 1);
//!     drop(db);
//!     jh.join().unwrap();
//! }
//! ```
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![feature(min_const_generics)]

// Note to developers: you can find decent overviews of the protocol at
//
//   https://github.com/cwarden/mysql-proxy/blob/master/doc/protocol.rst
//
// and
//
//   https://mariadb.com/kb/en/library/clientserver-protocol/
//
// Wireshark also does a pretty good job at parsing the MySQL protocol.

extern crate mysql_common as myc;

use std::collections::HashMap;
use std::io;
use std::io::prelude::*;
use std::iter;
use std::net;

use authentication::{generate_auth_data, hash_password};
use constants::{PROTOCOL_41, RESERVED, SECURE_CONNECTION};

pub use crate::myc::constants::{ColumnFlags, ColumnType, StatusFlags};

mod authentication;
mod commands;
mod constants;
mod errorcodes;
mod packet;
mod params;
mod resultset;
mod value;
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

pub use crate::errorcodes::ErrorKind;
pub use crate::params::{ParamParser, ParamValue, Params};
pub use crate::resultset::{InitWriter, QueryResultWriter, RowWriter, StatementMetaWriter};
pub use crate::value::{ToMysqlValue, Value, ValueInner};

/// Implementors of this trait can be used to drive a MySQL-compatible database backend.
pub trait MysqlShim<W: Write> {
    /// The error type produced by operations on this shim.
    ///
    /// Must implement `From<io::Error>` so that transport-level errors can be lifted.
    type Error: From<io::Error>;

    /// Called when the client issues a request to prepare `query` for later execution.
    ///
    /// The provided [`StatementMetaWriter`](struct.StatementMetaWriter.html) should be used to
    /// notify the client of the statement id assigned to the prepared statement, as well as to
    /// give metadata about the types of parameters and returned columns.
    fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client executes a previously prepared statement.
    ///
    /// Any parameters included with the client's command is given in `params`.
    /// A response to the query should be given using the provided
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client wishes to deallocate resources associated with a previously prepared
    /// statement.
    fn on_close(&mut self, stmt: u32);

    /// Called when the client issues a query for immediate execution.
    ///
    /// Results should be returned using the given
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), Self::Error>;

    /// Called when client switches database.
    fn on_init(&mut self, _: &str, _: InitWriter<'_, W>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Retrieve the password for the user with the given username, if any.
    ///
    /// If the user doesn't exist, return [`None`].
    fn password_for_username(&self, username: &[u8]) -> Option<Vec<u8>>;

    /// Return false if password checking should be skipped entirely
    fn require_authentication(&self) -> bool {
        true
    }
}

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements [`MysqlShim`](trait.MysqlShim.html).
pub struct MysqlIntermediary<B, R: Read, W: Write> {
    shim: B,
    reader: packet::PacketReader<R>,
    writer: packet::PacketWriter<W>,
}

impl<B: MysqlShim<net::TcpStream>> MysqlIntermediary<B, net::TcpStream, net::TcpStream> {
    /// Create a new server over a TCP stream and process client commands until the client
    /// disconnects or an error occurs. See also
    /// [`MysqlIntermediary::run_on`](struct.MysqlIntermediary.html#method.run_on).
    pub fn run_on_tcp(shim: B, stream: net::TcpStream) -> Result<(), B::Error> {
        let w = stream.try_clone()?;
        MysqlIntermediary::run_on(shim, stream, w)
    }
}

impl<B: MysqlShim<S>, S: Read + Write + Clone> MysqlIntermediary<B, S, S> {
    /// Create a new server over a two-way stream and process client commands until the client
    /// disconnects or an error occurs. See also
    /// [`MysqlIntermediary::run_on`](struct.MysqlIntermediary.html#method.run_on).
    pub fn run_on_stream(shim: B, stream: S) -> Result<(), B::Error> {
        MysqlIntermediary::run_on(shim, stream.clone(), stream)
    }
}

#[derive(Default)]
struct StatementData {
    long_data: HashMap<u16, Vec<u8>>,
    bound_types: Vec<(myc::constants::ColumnType, bool)>,
    params: u16,
}

const CAPABILITIES: u32 = PROTOCOL_41 | SECURE_CONNECTION | RESERVED;

impl<B: MysqlShim<W>, R: Read, W: Write> MysqlIntermediary<B, R, W> {
    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs.
    pub fn run_on(shim: B, reader: R, writer: W) -> Result<(), B::Error> {
        let r = packet::PacketReader::new(reader);
        let w = packet::PacketWriter::new(writer);
        let mut mi = MysqlIntermediary {
            shim,
            reader: r,
            writer: w,
        };
        if mi.init()? {
            mi.run()?;
        }
        Ok(())
    }

    fn init(&mut self) -> Result<bool, B::Error> {
        let auth_data = generate_auth_data();

        self.writer.write_all(&[10])?; // protocol 10

        // 5.1.10 because that's what Ruby's ActiveRecord requires
        self.writer.write_all(&b"5.1.10-alpha-msql-proxy\0"[..])?;

        self.writer.write_all(&[0x08, 0x00, 0x00, 0x00])?; // TODO: connection ID
        self.writer.write_all(&auth_data[..8])?;
        self.writer.write_all(&b"\0"[..])?;
        self.writer.write_all(&CAPABILITIES.to_le_bytes()[..2])?; // just 4.1 proto
        self.writer.write_all(&[0x21])?; // UTF8_GENERAL_CI
        self.writer.write_all(&[0x00, 0x00])?; // status flags
        self.writer.write_all(&CAPABILITIES.to_le_bytes()[2..])?; // extended capabilities
        self.writer.write_all(&[0x00])?; // no plugins
        self.writer.write_all(&[0x00; 6][..])?; // filler
        self.writer.write_all(&[0x00; 4][..])?; // filler
        self.writer.write_all(&auth_data[8..])?;
        self.writer.write_all(&b"\0"[..])?;
        self.writer.flush()?;

        let (seq, handshake_bytes) = self.reader.next()?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "peer terminated connection",
            )
        })?;
        let handshake = commands::client_handshake(&handshake_bytes)
            .map_err(|e| match e {
                nom::Err::Incomplete(_) => io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "client sent incomplete handshake",
                ),
                nom::Err::Failure((input, nom_e_kind)) | nom::Err::Error((input, nom_e_kind)) => {
                    if let nom::error::ErrorKind::Eof = nom_e_kind {
                        io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("client did not complete handshake; got {:?}", input),
                        )
                    } else {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("bad client handshake; got {:?} ({:?})", input, nom_e_kind),
                        )
                    }
                }
            })?
            .1;

        self.writer.set_seq(seq + 1);

        let auth_success = !self.shim.require_authentication()
            || self
                .shim
                .password_for_username(handshake.username)
                .map_or(false, |password| {
                    hash_password(&password, &auth_data) == handshake.password
                });

        if auth_success {
            writers::write_ok_packet(&mut self.writer, 0, 0, StatusFlags::empty())?;
        } else {
            writers::write_err(
                ErrorKind::ER_ACCESS_DENIED_ERROR,
                format!(
                    "Access denied for user {}",
                    String::from_utf8_lossy(handshake.username)
                )
                .as_bytes(),
                &mut self.writer,
            )?;
        }
        self.writer.flush()?;

        Ok(auth_success)
    }

    fn run(mut self) -> Result<(), B::Error> {
        use crate::commands::Command;

        let mut stmts: HashMap<u32, _> = HashMap::new();
        while let Some((seq, packet)) = self.reader.next()? {
            self.writer.set_seq(seq + 1);
            let cmd = commands::parse(&packet).unwrap().1;
            match cmd {
                Command::Query(q) => {
                    let w = QueryResultWriter::new(&mut self.writer, false);
                    if q.starts_with(b"SELECT @@") || q.starts_with(b"select @@") {
                        let var = &q[b"SELECT @@".len()..];
                        match var {
                            b"max_allowed_packet" => {
                                let cols = &[Column {
                                    table: String::new(),
                                    column: "@@max_allowed_packet".to_owned(),
                                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                                }];
                                let mut w = w.start(cols)?;
                                w.write_row(iter::once(67108864u32))?;
                                w.finish()?;
                            }
                            _ => {
                                w.completed(0, 0)?;
                            }
                        }
                    } else {
                        self.shim.on_query(
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )?;
                    }
                }
                Command::Prepare(q) => {
                    let w = StatementMetaWriter {
                        writer: &mut self.writer,
                        stmts: &mut stmts,
                    };

                    self.shim.on_prepare(
                        ::std::str::from_utf8(q)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        w,
                    )?;
                }
                Command::ResetStmtData(stmt) => {
                    stmts
                        .get_mut(&stmt)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("got reset data packet for unknown statement {}", stmt),
                            )
                        })?
                        .long_data
                        .clear();
                    writers::write_ok_packet(&mut self.writer, 0, 0, StatusFlags::empty())?;
                }
                Command::Execute { stmt, params } => {
                    let state = stmts.get_mut(&stmt).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("asked to execute unknown statement {}", stmt),
                        )
                    })?;
                    {
                        let params = params::ParamParser::new(params, state);
                        let w = QueryResultWriter::new(&mut self.writer, true);
                        self.shim.on_execute(stmt, params, w)?;
                    }
                    state.long_data.clear();
                }
                Command::SendLongData { stmt, param, data } => {
                    stmts
                        .get_mut(&stmt)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("got long data packet for unknown statement {}", stmt),
                            )
                        })?
                        .long_data
                        .entry(param)
                        .or_insert_with(Vec::new)
                        .extend(data);
                }
                Command::Close(stmt) => {
                    self.shim.on_close(stmt);
                    stmts.remove(&stmt);
                    // NOTE: spec dictates no response from server
                }
                Command::ListFields(_) => {
                    let cols = &[Column {
                        table: String::new(),
                        column: "not implemented".to_owned(),
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                        colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                    }];
                    writers::write_column_definitions(cols, &mut self.writer, true)?;
                }
                Command::Init(schema) => {
                    let w = InitWriter {
                        writer: &mut self.writer,
                    };
                    self.shim.on_init(
                        ::std::str::from_utf8(schema)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        w,
                    )?;
                }
                Command::Ping => {
                    writers::write_ok_packet(&mut self.writer, 0, 0, StatusFlags::empty())?;
                }
                Command::Quit => {
                    break;
                }
            }
            self.writer.flush()?;
        }
        Ok(())
    }
}
