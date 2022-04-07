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
//! # extern crate mysql_srv;
//! extern crate mysql;
//! extern crate mysql_common as myc;
//! # use std::io;
//! # use std::net;
//! # use std::thread;
//! use std::iter;
//!
//! use async_trait::async_trait;
//! use mysql::prelude::*;
//! use mysql_srv::*;
//! use tokio::io::AsyncWrite;
//!
//! struct Backend;
//! #[async_trait]
//! impl<W: AsyncWrite + Unpin + Send + 'static> MysqlShim<W> for Backend {
//!     async fn on_prepare(
//!         &mut self,
//!         _: &str,
//!         info: StatementMetaWriter<'_, W>,
//!     ) -> io::Result<()> {
//!         info.reply(42, &[], &[]).await
//!     }
//!     async fn on_execute(
//!         &mut self,
//!         _: u32,
//!         _: ParamParser<'_>,
//!         results: QueryResultWriter<'_, W>,
//!     ) -> io::Result<()> {
//!         results.completed(0, 0, None).await
//!     }
//!     async fn on_close(&mut self, _: u32) {}
//!
//!     async fn on_init(&mut self, _: &str, w: InitWriter<'_, W>) -> io::Result<()> {
//!         w.ok().await
//!     }
//!
//!     async fn on_query(
//!         &mut self,
//!         query: &str,
//!         results: QueryResultWriter<'_, W>,
//!     ) -> io::Result<()> {
//!         if query.starts_with("SELECT @@") || query.starts_with("select @@") {
//!             let var = &query.get(b"SELECT @@".len()..);
//!             return match var {
//!                 Some("max_allowed_packet") => {
//!                     let cols = &[Column {
//!                         table: String::new(),
//!                         column: "@@max_allowed_packet".to_owned(),
//!                         coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
//!                         colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
//!                     }];
//!                     let mut w = results.start(cols).await?;
//!                     w.write_row(iter::once(67108864u32))?;
//!                     Ok(w.finish().await?)
//!                 }
//!                 _ => Ok(results.completed(0, 0, None).await?),
//!             };
//!         } else {
//!             let cols = [
//!                 Column {
//!                     table: "foo".to_string(),
//!                     column: "a".to_string(),
//!                     coltype: ColumnType::MYSQL_TYPE_LONGLONG,
//!                     colflags: ColumnFlags::empty(),
//!                 },
//!                 Column {
//!                     table: "foo".to_string(),
//!                     column: "b".to_string(),
//!                     coltype: ColumnType::MYSQL_TYPE_STRING,
//!                     colflags: ColumnFlags::empty(),
//!                 },
//!             ];
//!
//!             let mut rw = results.start(&cols).await?;
//!             rw.write_col(42)?;
//!             rw.write_col("b's value")?;
//!             rw.finish().await
//!         }
//!     }
//!
//!     fn password_for_username(&self, _username: &str) -> Option<Vec<u8>> {
//!         Some(b"password".to_vec())
//!     }
//! }
//!
//! fn main() {
//!     let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
//!     let port = listener.local_addr().unwrap().port();
//!     let mut rt = tokio::runtime::Runtime::new().unwrap();
//!
//!     let jh = thread::spawn(move || {
//!         if let Ok((s, _)) = listener.accept() {
//!             let s = {
//!                 let _guard = rt.handle().enter();
//!                 tokio::net::TcpStream::from_std(s).unwrap()
//!             };
//!             rt.block_on(MysqlIntermediary::run_on_tcp(Backend, s))
//!                 .unwrap();
//!         }
//!     });
//!
//!     let mut db = mysql::Conn::new(
//!         mysql::Opts::from_url(&format!("mysql://root:password@127.0.0.1:{}", port)).unwrap(),
//!     )
//!     .unwrap();
//!     assert_eq!(db.ping(), true);
//!     assert_eq!(
//!         db.query::<mysql::Row, _>("SELECT a, b FROM foo")
//!             .unwrap()
//!             .len(),
//!         1
//!     );
//!     drop(db);
//!     jh.join().unwrap();
//! }
//! ```
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![feature(io_slice_advance)]

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

use async_trait::async_trait;
use constants::{CLIENT_PLUGIN_AUTH, PROTOCOL_41, RESERVED, SECURE_CONNECTION};
use error::{other_error, OtherErrorKind};
use mysql_common::constants::CapabilityFlags;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net;
use tracing::debug;
use writers::write_err;

use crate::authentication::{generate_auth_data, hash_password, AUTH_PLUGIN_NAME};
pub use crate::myc::constants::{ColumnFlags, ColumnType, StatusFlags};
pub use crate::writers::prepare_column_definitions;

mod authentication;
mod commands;
mod constants;
pub mod error;
mod errorcodes;
mod packet;
mod params;
mod resultset;
mod value;
mod writers;

/// CURRENT_VERSION is relayed back to the client as the current server version. Most clients will
/// interpret the version numbers and use that to dictate which dialect they send us. Anything
/// after the version can be any text we desire. If you change this, feel free to change the byte
/// array length as necessary. The length is not a crucial component.
const CURRENT_VERSION: &[u8; 16] = b"8.0.26-readyset\0";

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

impl From<&mysql_async::Column> for Column {
    fn from(c: &mysql_async::Column) -> Self {
        Column {
            table: c.table_str().to_string(),
            column: c.name_str().to_string(),
            coltype: c.column_type(),
            colflags: c.flags(),
        }
    }
}

pub use crate::error::MsqlSrvError;
pub use crate::errorcodes::ErrorKind;
pub use crate::params::{ParamParser, ParamValue, Params};
pub use crate::resultset::{InitWriter, QueryResultWriter, RowWriter, StatementMetaWriter};
pub use crate::value::{ToMysqlValue, Value, ValueInner};

/// Implementors of this trait can be used to drive a MySQL-compatible database backend.
#[async_trait]
pub trait MysqlShim<W: AsyncWrite + Unpin + Send> {
    /// Called when the client issues a request to prepare `query` for later execution.
    ///
    /// The provided [`StatementMetaWriter`](struct.StatementMetaWriter.html) should be used to
    /// notify the client of the statement id assigned to the prepared statement, as well as to
    /// give metadata about the types of parameters and returned columns.
    async fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<'_, W>)
        -> io::Result<()>;

    /// Called when the client executes a previously prepared statement.
    ///
    /// Any parameters included with the client's command is given in `params`.
    /// A response to the query should be given using the provided
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    async fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()>;

    /// Called when the client wishes to deallocate resources associated with a previously prepared
    /// statement.
    async fn on_close(&mut self, stmt: u32);

    /// Called when the client issues a query for immediate execution.
    ///
    /// Results should be returned using the given
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    async fn on_query(&mut self, query: &str, results: QueryResultWriter<'_, W>) -> io::Result<()>;

    /// Called when client switches database.
    async fn on_init(&mut self, _: &str, _: InitWriter<'_, W>) -> io::Result<()>;

    /// Retrieve the password for the user with the given username, if any.
    ///
    /// If the user doesn't exist, return [`None`].
    fn password_for_username(&self, username: &str) -> Option<Vec<u8>>;

    /// Return false if password checking should be skipped entirely
    fn require_authentication(&self) -> bool {
        true
    }
}

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements [`MysqlShim`](trait.MysqlShim.html).
pub struct MysqlIntermediary<B, R: AsyncRead + Unpin, W: AsyncWrite + Unpin> {
    shim: B,
    reader: packet::PacketReader<R>,
    writer: packet::PacketWriter<W>,
}

impl<B: MysqlShim<net::tcp::OwnedWriteHalf> + Send>
    MysqlIntermediary<B, net::TcpStream, net::TcpStream>
{
    /// Create a new server over a TCP stream and process client commands until the client
    /// disconnects or an error occurs. See also
    /// [`MysqlIntermediary::run_on`](struct.MysqlIntermediary.html#method.run_on).
    pub async fn run_on_tcp(shim: B, stream: net::TcpStream) -> Result<(), io::Error> {
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        MysqlIntermediary::run_on(shim, reader, writer).await
    }
}

impl<B: MysqlShim<S> + Send, S: AsyncRead + AsyncWrite + Clone + Unpin + Send>
    MysqlIntermediary<B, S, S>
{
    /// Create a new server over a two-way stream and process client commands until the client
    /// disconnects or an error occurs. See also
    /// [`MysqlIntermediary::run_on`](struct.MysqlIntermediary.html#method.run_on).
    pub async fn run_on_stream(shim: B, stream: S) -> Result<(), io::Error> {
        MysqlIntermediary::run_on(shim, stream.clone(), stream).await
    }
}

/// Send an error packet to the given stream, then close it
pub async fn send_immediate_err<S>(stream: S, error_kind: ErrorKind, msg: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin + Send,
{
    let mut w = packet::PacketWriter::new(stream);
    write_err(error_kind, msg, &mut w).await
}

#[derive(Default)]
struct StatementData {
    long_data: HashMap<u16, Vec<u8>>,
    bound_types: Vec<(myc::constants::ColumnType, bool)>,
    params: u16,
}

const CAPABILITIES: u32 = PROTOCOL_41 | SECURE_CONNECTION | RESERVED | CLIENT_PLUGIN_AUTH;

impl<B: MysqlShim<W> + Send, R: AsyncRead + Unpin, W: AsyncWrite + Unpin + Send>
    MysqlIntermediary<B, R, W>
{
    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs.
    pub async fn run_on(shim: B, reader: R, writer: W) -> Result<(), io::Error> {
        let r = packet::PacketReader::new(reader);
        let w = packet::PacketWriter::new(writer);
        let mut mi = MysqlIntermediary {
            shim,
            reader: r,
            writer: w,
        };
        if mi.init().await? {
            mi.run().await?;
        }
        Ok(())
    }

    async fn init(&mut self) -> Result<bool, io::Error> {
        let auth_data =
            generate_auth_data().map_err(|_| other_error(OtherErrorKind::AuthDataErr))?;

        let mut init_packet = Vec::with_capacity(
            1 + 16 + 4 + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 6 + 4 + 12 + 1 + AUTH_PLUGIN_NAME.len() + 1,
        );
        init_packet.extend_from_slice(&[10]); // protocol 10
        init_packet.extend_from_slice(CURRENT_VERSION);
        init_packet.extend_from_slice(&[0x08, 0x00, 0x00, 0x00]); // TODO: connection ID
        init_packet.extend_from_slice(&auth_data[..8]);
        init_packet.push(0);
        init_packet.extend_from_slice(&CAPABILITIES.to_le_bytes()[..2]);
        init_packet.extend_from_slice(&[0x21]); // UTF8_GENERAL_CI
        init_packet.extend_from_slice(&[0x00, 0x00]); // status flags
        init_packet.extend_from_slice(&CAPABILITIES.to_le_bytes()[2..]);
        init_packet.extend_from_slice(&[auth_data.len() as u8]);
        init_packet.extend_from_slice(&[0x00; 10][..]); // filler
        init_packet.extend_from_slice(&auth_data[8..]);
        init_packet.push(0);
        init_packet.extend_from_slice(AUTH_PLUGIN_NAME.as_bytes());
        init_packet.push(0);

        self.writer.write_packet(&init_packet).await?;
        self.writer.flush().await?;

        let (seq, handshake_bytes) = self.reader.next().await?.ok_or_else(|| {
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
                nom::Err::Failure(nom::error::Error { input, code })
                | nom::Err::Error(nom::error::Error { input, code }) => {
                    if let nom::error::ErrorKind::Eof = code {
                        io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("client did not complete handshake; got {:?}", input),
                        )
                    } else {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("bad client handshake; got {:?} ({:?})", input, code),
                        )
                    }
                }
            })?
            .1;

        self.writer.set_seq(seq + 1);

        let username = handshake.username.to_owned();
        let password = handshake.password.to_vec();
        let client_auth_plugin = handshake.auth_plugin_name.map(|s| s.to_owned());

        let handshake_password = if client_auth_plugin.iter().any(|apn| apn != AUTH_PLUGIN_NAME) {
            // Authentication mismatch - try to switch auth plugins

            if !handshake
                .capabilities
                .contains(CapabilityFlags::CLIENT_SECURE_CONNECTION)
            {
                debug!("Client does not support SECURE_CONNECTION, returning authentication error");
                writers::write_err(
                    ErrorKind::ER_NOT_SUPPORTED_AUTH_MODE,
                    b"Client does not support authentication protocol requested by server; \
                      consider upgrading MySQL client",
                    &mut self.writer,
                )
                .await?;
                return Ok(false);
            }

            debug!(
                ?client_auth_plugin,
                "Client offered incorrect authentication plugin, sending switch request",
            );

            let mut auth_switch_request_packet =
                Vec::with_capacity(1 + AUTH_PLUGIN_NAME.len() + 1 + auth_data.len() + 1);
            auth_switch_request_packet.push(0xfe);
            auth_switch_request_packet.extend_from_slice(AUTH_PLUGIN_NAME.as_bytes());
            auth_switch_request_packet.push(0);
            auth_switch_request_packet.extend_from_slice(&auth_data);
            auth_switch_request_packet.push(0);
            self.writer
                .write_packet(&auth_switch_request_packet)
                .await?;
            self.writer.flush().await?;

            let (seq, auth_switch_response) = self.reader.next().await?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "peer terminated connection",
                )
            })?;
            self.writer.set_seq(seq + 1);

            auth_switch_response.to_vec()
        } else {
            password
        };

        let auth_success = !self.shim.require_authentication()
            || self
                .shim
                .password_for_username(&username)
                .map_or(false, |password| {
                    hash_password(&password, &auth_data) == handshake_password.as_slice()
                });

        if auth_success {
            writers::write_ok_packet(&mut self.writer, 0, 0, StatusFlags::empty()).await?;
        } else {
            debug!(%username, ?client_auth_plugin, "Received incorrect password");
            writers::write_err(
                ErrorKind::ER_ACCESS_DENIED_ERROR,
                format!("Access denied for user {}", username).as_bytes(),
                &mut self.writer,
            )
            .await?;
        }
        self.writer.flush().await?;

        Ok(auth_success)
    }

    async fn run(mut self) -> Result<(), io::Error> {
        use crate::commands::Command;

        let mut stmts: HashMap<u32, _> = HashMap::new();
        while let Some((seq, packet)) = self.reader.next().await? {
            self.writer.set_seq(seq + 1);
            let cmd = commands::parse(&packet)
                .map_err(|e| {
                    other_error(OtherErrorKind::GenericErr {
                        error: format!("{:?}", e),
                    })
                })?
                .1;
            match cmd {
                Command::Query(q) => {
                    let w = QueryResultWriter::new(&mut self.writer, false);
                    self.shim
                        .on_query(
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )
                        .await?;
                }
                Command::Prepare(q) => {
                    let w = StatementMetaWriter {
                        writer: &mut self.writer,
                        stmts: &mut stmts,
                    };
                    self.shim
                        .on_prepare(
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )
                        .await?;
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
                    writers::write_ok_packet(&mut self.writer, 0, 0, StatusFlags::empty()).await?;
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
                        self.shim.on_execute(stmt, params, w).await?;
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
                    self.shim.on_close(stmt).await;
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
                    writers::write_column_definitions(cols, &mut self.writer, true).await?;
                }
                Command::Init(schema) => {
                    let w = InitWriter {
                        writer: &mut self.writer,
                    };
                    self.shim
                        .on_init(
                            ::std::str::from_utf8(schema)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )
                        .await?;
                }
                Command::Ping => {
                    writers::write_ok_packet(&mut self.writer, 0, 0, StatusFlags::empty()).await?;
                    self.writer.flush().await?;
                }
                Command::Quit => {
                    break;
                }
            }

            self.writer.flush().await?;
        }

        Ok(())
    }
}
