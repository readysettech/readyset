//! Bindings for emulating a MySQL/MariaDB server.
//!
//! When developing new databases or caching layers, it can be immensely useful to test your system
//! using existing applications. However, this often requires significant work modifying
//! applications to use your database over the existing ones. This crate solves that problem by
//! acting as a MySQL server, and delegating operations such as querying and query execution to
//! user-defined logic.
//!
//! To start, implement [`MySqlShim`] for your backend, and create a [`MySqlIntermediary`] over an
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
//! use std::collections::HashMap;
//! use std::iter;
//!
//! use readyset_util::redacted::RedactedString;
//! use database_utils::TlsMode;
//! use mysql::prelude::*;
//! use mysql_srv::*;
//! use readyset_adapter_types::DeallocateId;
//! use tokio::io::{AsyncRead, AsyncWrite};
//!
//! struct Backend;
//! impl<W: AsyncRead + AsyncWrite + Unpin + Send + 'static> MySqlShim<W> for Backend {
//!     async fn on_prepare(
//!         &mut self,
//!         _: &str,
//!         info: StatementMetaWriter<'_, W>,
//!         schema_cache: &mut HashMap<u32, CachedSchema>,
//!     ) -> io::Result<()> {
//!         info.reply(42, &[], &[]).await
//!     }
//!     async fn on_execute(
//!         &mut self,
//!         _: u32,
//!         _: ParamParser<'_>,
//!         results: QueryResultWriter<'_, W>,
//!         schema_cache: &mut HashMap<u32, CachedSchema>,
//!     ) -> io::Result<()> {
//!         results.completed(0, 0, None).await
//!     }
//!     async fn on_close(&mut self, _: DeallocateId) {}
//!
//!     async fn on_ping(&mut self) -> io::Result<()> {
//!         Ok(())
//!     }
//!
//!     async fn on_reset(&mut self) -> io::Result<()> {
//!         Ok(())
//!     }
//!
//!     async fn on_init(&mut self, _: &str, w: Option<InitWriter<'_, W>>) -> io::Result<()> {
//!         w.unwrap().ok().await
//!     }
//!     async fn on_change_user(&mut self, _: &str, _: &str, _: &str) -> io::Result<()> {
//!         Ok(())
//!     }
//!
//!     async fn set_auth_info(&mut self, _: &str, _: Option<RedactedString>) -> io::Result<()> {
//!         Ok(())
//!     }
//!
//!     async fn on_query(
//!         &mut self,
//!         query: &str,
//!         results: QueryResultWriter<'_, W>,
//!     ) -> QueryResultsResponse {
//!         if query.starts_with("SELECT @@") || query.starts_with("select @@") {
//!             let var = &query.get(b"SELECT @@".len()..);
//!             return match var {
//!                 Some("max_allowed_packet") => {
//!                     let cols = &[Column {
//!                         table: String::new(),
//!                         column: "@@max_allowed_packet".to_owned(),
//!                         coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
//!                         column_length: None,
//!                         colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
//!                         character_set: myc::constants::UTF8_GENERAL_CI,
//!                     }];
//!                     let mut w = results.start(cols).await.expect("cols");
//!                     w.write_row(iter::once(67108864u32)).await.expect("writer");
//!                     QueryResultsResponse::IoResult(w.finish().await)
//!                 }
//!                 _ => QueryResultsResponse::IoResult(results.completed(0, 0, None).await),
//!             };
//!         } else {
//!             let cols = [
//!                 Column {
//!                     table: "foo".to_string(),
//!                     column: "a".to_string(),
//!                     coltype: ColumnType::MYSQL_TYPE_LONGLONG,
//!                     column_length: None,
//!                     colflags: ColumnFlags::empty(),
//!                     character_set: myc::constants::UTF8_GENERAL_CI,
//!                 },
//!                 Column {
//!                     table: "foo".to_string(),
//!                     column: "b".to_string(),
//!                     coltype: ColumnType::MYSQL_TYPE_STRING,
//!                     column_length: None,
//!                     colflags: ColumnFlags::empty(),
//!                     character_set: myc::constants::UTF8_GENERAL_CI,
//!                 },
//!             ];
//!
//!             let mut rw = results.start(&cols).await.expect("cols");
//!             rw.write_col(42).expect("writer");
//!             rw.write_col("b's value").expect("writer");
//!             QueryResultsResponse::IoResult(rw.finish().await)
//!         }
//!     }
//!
//!     fn password_for_username(&self, _username: &str) -> Option<Vec<u8>> {
//!         Some(b"password".to_vec())
//!     }
//!
//!     fn version(&self) -> String {
//!         "8.0.31-readyset\0".to_string()
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
//!             rt.block_on(MySqlIntermediary::run_on_tcp(Backend, s, false, None, TlsMode::Optional))
//!                 .unwrap();
//!         }
//!     });
//!
//!     let mut db = mysql::Conn::new(
//!         mysql::Opts::from_url(&format!("mysql://root:password@127.0.0.1:{}", port)).unwrap(),
//!     )
//!     .unwrap();
//!     assert!(db.ping().is_ok());
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
use std::sync::Arc;

use constants::{
    CLIENT_PLUGIN_AUTH, CONNECT_WITH_DB, LONG_PASSWORD, PROTOCOL_41, RESERVED, SECURE_CONNECTION,
    SSL,
};
use database_utils::TlsMode;
use error::{other_error, OtherErrorKind};
use mysql_common::constants::CapabilityFlags;
use readyset_adapter_types::{DeallocateId, ParsedCommand};
use readyset_data::DfType;
use readyset_util::redacted::RedactedString;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net;
use tokio_native_tls::TlsAcceptor;
use tracing::{debug, info, trace};
use writers::write_err;

use crate::authentication::{generate_auth_data, hash_password, AUTH_PLUGIN_NAME};
use crate::commands::change_user;
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
mod tls;
mod value;
mod writers;

/// Meta-information about a single column, used either to describe a prepared statement parameter
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
    /// This column's type.
    pub coltype: ColumnType,
    /// This column's display length.
    pub column_length: Option<u32>,
    /// Holds the character set for this column.
    pub character_set: u16,
    /// Any flags associated with this column.
    ///
    /// Of particular interest are [`ColumnFlags::UNSIGNED_FLAG`] and [`ColumnFlags::NOT_NULL_FLAG`]
    pub colflags: ColumnFlags,
}

impl From<&mysql_async::Column> for Column {
    fn from(c: &mysql_async::Column) -> Self {
        Column {
            table: c.table_str().to_string(),
            column: c.name_str().to_string(),
            coltype: c.column_type(),
            column_length: Some(c.column_length()),
            character_set: c.character_set(),
            colflags: c.flags(),
        }
    }
}

pub use crate::error::MsqlSrvError;
pub use crate::errorcodes::ErrorKind;
pub use crate::params::{ParamParser, ParamValue, Params};
pub use crate::resultset::{InitWriter, QueryResultWriter, RowWriter, StatementMetaWriter};
pub use crate::value::{ToMySqlValue, Value, ValueInner};

/// A wrapper to allow either an [`io::Result`] or a [`ParsedCommand`] to be returned
pub enum QueryResultsResponse {
    /// Variant for an [`io::Result`]
    IoResult(io::Result<()>),
    /// A parsed SQL command, like `DEALLOCATE`
    Command(ParsedCommand),
}

/// Implementors of this trait can be used to drive a MySQL-compatible database backend.
// Only used internally
#[allow(async_fn_in_trait)]
pub trait MySqlShim<S: AsyncRead + AsyncWrite + Unpin + Send> {
    /// Called when the client issues a request to prepare `query` for later execution.
    ///
    /// The provided [`StatementMetaWriter`] should be used to notify the client of the statement id
    /// assigned to the prepared statement, as well as to give metadata about the types of
    /// parameters and returned columns.
    async fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, S>,
        schema_cache: &mut HashMap<u32, CachedSchema>,
    ) -> io::Result<()>;

    /// Provides the server's version information along with Readyset indications
    fn version(&self) -> String;

    /// Called when the client executes a previously prepared statement.
    ///
    /// Any parameters included with the client's command is given in `params`. A response to the
    /// query should be given using the provided [`QueryResultWriter`].
    async fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, S>,
        schema_cache: &mut HashMap<u32, CachedSchema>,
    ) -> io::Result<()>;

    /// Called when the client wishes to deallocate resources associated with a previously prepared
    /// statement.
    async fn on_close(&mut self, stmt: DeallocateId);

    /// Called when the client issues a query for immediate execution.
    ///
    /// Results should be returned using the given [`QueryResultWriter`].
    async fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, S>,
    ) -> QueryResultsResponse;

    /// Called when the client issue a ping command.
    async fn on_ping(&mut self) -> io::Result<()>;

    /// Called when the client issues a reset command
    async fn on_reset(&mut self) -> io::Result<()>;

    /// Called when client switches database.
    async fn on_init(&mut self, _: &str, _: Option<InitWriter<'_, S>>) -> io::Result<()>;

    /// Called when client switches user.
    async fn on_change_user(&mut self, _: &str, _: &str, _: &str) -> io::Result<()>;

    /// Called when client authenticates to inform which users we should use.
    async fn set_auth_info(&mut self, _: &str, _: Option<RedactedString>) -> io::Result<()>;

    /// Retrieve the password for the user with the given username, if any.
    ///
    /// If the user doesn't exist, return [`None`].
    fn password_for_username(&self, username: &str) -> Option<Vec<u8>>;

    /// Return false if password checking should be skipped entirely
    fn require_authentication(&self) -> bool {
        true
    }
}

/// Stores a preencoded result schema for a prepared MySQL statement
pub struct CachedSchema {
    /// The MySQL schema
    pub mysql_schema: Vec<Column>,
    /// Associated Readyset types
    pub column_types: Vec<DfType>,
    /// Preencoded schema as a byte dump
    pub preencoded_schema: Arc<[u8]>,
}

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements [`MySqlShim`].
pub struct MySqlIntermediary<B, S: AsyncRead + AsyncWrite + Unpin> {
    shim: B,
    conn: packet::PacketConn<S>,
    /// A cache of schemas per statement id
    schema_cache: HashMap<u32, CachedSchema>,
    /// Whether to log statements received from a client
    enable_statement_logging: bool,
    /// The capabilities of the client
    client_capabilities: CapabilityFlags,
    /// Auth data sent to client
    auth_data: [u8; 20],
    /// TLS acceptor
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    // Tls mode
    tls_mode: TlsMode,
}

impl<B: MySqlShim<net::TcpStream> + Send> MySqlIntermediary<B, net::TcpStream> {
    /// Create a new server over a TCP stream and process client commands until the client
    /// disconnects or an error occurs. See also [`MySqlIntermediary::run_on`].
    pub async fn run_on_tcp(
        shim: B,
        stream: net::TcpStream,
        enable_statement_logging: bool,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
        tls_mode: TlsMode,
    ) -> Result<(), io::Error> {
        stream.set_nodelay(true)?;
        MySqlIntermediary::run_on(
            shim,
            stream,
            enable_statement_logging,
            tls_acceptor,
            tls_mode,
        )
        .await
    }
}

impl<B: MySqlShim<S> + Send, S: AsyncRead + AsyncWrite + Unpin + Send> MySqlIntermediary<B, S> {
    /// Create a new server over a stream and process client commands until the client
    /// disconnects or an error occurs. See also [`MySqlIntermediary::run_on`].
    pub async fn run_on_stream(
        shim: B,
        stream: S,
        enable_statement_logging: bool,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
        tls_mode: TlsMode,
    ) -> Result<(), io::Error> {
        MySqlIntermediary::run_on(
            shim,
            stream,
            enable_statement_logging,
            tls_acceptor,
            tls_mode,
        )
        .await
    }
}

/// Send an error packet to the given stream, then close it
pub async fn send_immediate_err<S>(stream: S, error_kind: ErrorKind, msg: &[u8]) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut conn = packet::PacketConn::new(stream);
    write_err(error_kind, msg, &mut conn).await
}

#[derive(Default)]
struct StatementData {
    long_data: HashMap<u16, Vec<u8>>,
    bound_types: Vec<(myc::constants::ColumnType, bool)>,
    params: u16,
}

const CAPABILITIES: u32 = PROTOCOL_41
    | LONG_PASSWORD
    | SECURE_CONNECTION
    | RESERVED
    | CLIENT_PLUGIN_AUTH
    | CONNECT_WITH_DB;

impl<B: MySqlShim<S> + Send, S: AsyncWrite + AsyncRead + Unpin + Send> MySqlIntermediary<B, S> {
    /// Create a new server over a channel and process client commands until the client
    /// disconnects or an error occurs.
    pub async fn run_on(
        shim: B,
        stream: S,
        enable_statement_logging: bool,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
        tls_mode: TlsMode,
    ) -> Result<(), io::Error> {
        let mut mi = MySqlIntermediary {
            shim,
            conn: packet::PacketConn::new(stream),
            schema_cache: HashMap::new(),
            enable_statement_logging,
            client_capabilities: CapabilityFlags::empty(),
            auth_data: [0; 20],
            tls_acceptor,
            tls_mode,
        };
        if let (true, username, password, database) = mi.init().await? {
            mi.shim.set_auth_info(&username, password).await?;
            if let Some(database) = database {
                mi.shim.on_init(&database, None).await?;
            }
            mi.run().await?;
        }
        Ok(())
    }

    /// Handle the client handshake messages for establishing capabilities and handling
    /// authentication.
    ///
    /// First build a HandshakeV10 packet to send to the client, then attempt to receive and parse
    /// the HandshakeResponse packet that the client should send back to us. More packets may be
    /// sent and received as needed to complete authentication.
    ///
    /// If no errors are encountered, the return value contains a tuple of a boolean to indicate
    /// whether authentication was successful, the username, the plaintext password if one was
    /// provided, and a database name if one was specified by the client in the handshake response.
    async fn init(
        &mut self,
    ) -> Result<(bool, String, Option<RedactedString>, Option<String>), io::Error> {
        let auth_data =
            generate_auth_data().map_err(|_| other_error(OtherErrorKind::AuthDataErr))?;
        self.auth_data = auth_data;
        let mut init_packet = Vec::with_capacity(
            1 + 16 + 4 + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 6 + 4 + 12 + 1 + AUTH_PLUGIN_NAME.len() + 1,
        );
        init_packet.extend_from_slice(&[10]); // protocol 10
        init_packet.extend_from_slice(self.shim.version().as_bytes());
        init_packet.extend_from_slice(&[0x08, 0x00, 0x00, 0x00]); // TODO: connection ID
        init_packet.extend_from_slice(&auth_data[..8]);
        init_packet.push(0);

        // We will check if the pkcs12 file was correctly provided and we have a functioning
        // a TlsAcceptor. If TlsAcceptor is available, we will add SSL capabilities to the
        // init packet.
        let mut capabilities = CAPABILITIES;
        if self.tls_acceptor.is_some() && self.tls_mode != TlsMode::Disabled {
            capabilities |= SSL; // SSL support flag
        }
        init_packet.extend_from_slice(&capabilities.to_le_bytes()[..2]);

        init_packet.extend_from_slice(&[0x21]); // UTF8_GENERAL_CI
        init_packet.extend_from_slice(&[0x00, 0x00]); // status flags
        init_packet.extend_from_slice(&CAPABILITIES.to_le_bytes()[2..]);
        // We will add a \0 byte below so we need to account for that when sending the length, since
        // rust strings don't add the null terminator
        init_packet.extend_from_slice(&[(auth_data.len() + 1) as u8]);
        init_packet.extend_from_slice(&[0x00; 10][..]); // filler
        init_packet.extend_from_slice(&auth_data[8..]);
        init_packet.push(0);
        init_packet.extend_from_slice(AUTH_PLUGIN_NAME.as_bytes());
        init_packet.push(0);

        self.conn.enqueue_packet(init_packet);
        self.conn.flush().await?;

        let mut packet = self.conn.next().await?.ok_or_else(|| {
            // We use the stdlib's "custom" [`io::ErrorKind`] for this expected/benign error that
            // occurs during a Layer 4 network health check, to indicate it can be ignored higher up
            io::Error::new(
                io::ErrorKind::Other,
                "peer terminated connection before sending bytes",
            )
        })?;

        // Peek at the first 4 bytes (the capabilities flags) without consuming them
        let capabilities = &packet.data[..4];
        if commands::is_ssl_request(capabilities)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "invalid client capabilities flags in the handshake",
                )
            })?
            .1
        {
            // switch to ssl
            self.tls_acceptor.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "TLS acceptor not set")
            })?;

            self.conn
                .stream
                .switch_to_tls(self.tls_acceptor.clone().unwrap())
                .await?;

            // The connection has been switched to TLS successfully. Read the handshake
            // again as per TLS handshake protocol.
            self.conn.set_seq(packet.seq + 1);
            packet = self.conn.next().await?.ok_or_else(|| {
                // We use the stdlib's "custom" [`io::ErrorKind`] for this expected/benign error that
                // occurs during a Layer 4 network health check, to indicate it can be ignored higher up
                io::Error::new(
                    io::ErrorKind::Other,
                    "peer terminated connection before sending handshake after TLS connection",
                )
            })?;
        } else {
            // Client connected using a non encrypted stream. Write an error if TLS mode is required.
            if self.tls_mode == TlsMode::Required {
                self.conn.set_seq(packet.seq + 1);
                writers::write_err(
                    ErrorKind::ER_SECURE_TRANSPORT_REQUIRED,
                    b"Connections using insecure transport are prohibited.",
                    &mut self.conn,
                )
                .await?;
                self.conn.flush().await?;
                return Ok((false, "".to_string(), None, None));
            }
        }

        let handshake = commands::client_handshake(&packet.data)
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

        self.conn.set_seq(packet.seq + 1);

        self.client_capabilities = handshake.capabilities;
        let username = handshake.username.to_owned();
        let password = handshake.password.to_vec();
        let database = handshake.database.map(String::from);
        let client_auth_plugin = handshake.auth_plugin_name.map(|s| s.to_owned());

        let handshake_password = if client_auth_plugin.iter().all(|apn| apn != AUTH_PLUGIN_NAME)
            // Some clients (at the very least certain versions of PHP's MySQL PDO library) send an
            // empty password response in the initial handshake, even if the auth plugin is set and
            // correct. We want to send a switch-authentication request in that case too
            || password.is_empty()
        {
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
                    &mut self.conn,
                )
                .await?;
                return Ok((false, "".to_string(), None, database));
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
            self.conn.enqueue_packet(auth_switch_request_packet);
            self.conn.flush().await?;

            let packet = self.conn.next().await?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "peer terminated connection when asked to switch auth plugin",
                )
            })?;
            self.conn.set_seq(packet.seq + 1);

            packet.data.to_vec()
        } else {
            password
        };

        let plain_password = self.shim.password_for_username(&username);
        let require_auth = self.shim.require_authentication();
        let auth_success = !require_auth
            || plain_password.as_ref().is_some_and(|password| {
                let expected = hash_password(password, &auth_data);
                let actual = handshake_password.as_slice();
                trace!(?expected, ?actual);
                expected == actual
            });
        let plain_password = if require_auth {
            Some(RedactedString::from(
                plain_password
                    .map(|p| String::from_utf8_lossy(&p).into_owned())
                    .unwrap_or_default(),
            ))
        } else {
            None
        };
        if auth_success {
            debug!(%username, "Successfully authenticated client");
            writers::write_ok_packet(&mut self.conn, 0, 0, StatusFlags::empty()).await?;
        } else {
            debug!(%username, ?client_auth_plugin, "Received incorrect password");
            writers::write_err(
                ErrorKind::ER_ACCESS_DENIED_ERROR,
                format!("Access denied for user {}", username).as_bytes(),
                &mut self.conn,
            )
            .await?;
        }
        self.conn.flush().await?;
        Ok((auth_success, username, plain_password, database))
    }

    async fn run(mut self) -> Result<(), io::Error> {
        use crate::commands::Command;

        let mut stmts: HashMap<u32, _> = HashMap::new();
        while let Some(packet) = self.conn.next().await? {
            self.conn.set_seq(packet.seq + 1);
            let cmd = commands::parse(&packet)
                .map_err(|e| {
                    other_error(OtherErrorKind::GenericErr {
                        error: format!("{:?}", e),
                    })
                })?
                .1;
            // These other variants are logged by the readyset-mysql `Backend`.
            if self.enable_statement_logging
                && !matches!(
                    cmd,
                    Command::Query(_)
                        | Command::Prepare(_)
                        | Command::Execute { .. }
                        | Command::Init(_)
                )
            {
                info!(target: "client_statement", "{:?}", cmd);
            }
            match cmd {
                Command::ChangeUser(q) => {
                    let change_user = change_user(q, self.client_capabilities)
                        .map_err(|e| {
                            other_error(OtherErrorKind::GenericErr {
                                error: format!("{:?}", e),
                            })
                        })?
                        .1;
                    let username = change_user.username.to_owned();
                    let authpassword = change_user.password.to_vec();

                    if change_user.auth_plugin_name != AUTH_PLUGIN_NAME {
                        // This should never happen, as we already accepted a connection using
                        // AUTH_PLUGIN_NAME
                        writers::write_err(
                            ErrorKind::ER_ACCESS_DENIED_ERROR,
                            format!(
                                "Access denied for user {}. Incorrect auth plugin {}",
                                username, change_user.auth_plugin_name
                            )
                            .as_bytes(),
                            &mut self.conn,
                        )
                        .await?;
                        self.conn.flush().await?;
                        continue;
                    }
                    let plain_password = self.shim.password_for_username(&username);
                    let auth_success = !self.shim.require_authentication()
                        || plain_password.as_ref().is_some_and(|password| {
                            let expected = hash_password(password, &self.auth_data);
                            let actual = authpassword.as_slice();
                            trace!(?expected, ?actual);
                            expected == actual
                        });

                    if auth_success {
                        debug!("Successfully authenticated client");
                        match self
                            .shim
                            .on_change_user(
                                &username,
                                &plain_password
                                    .as_ref()
                                    .map(|p| String::from_utf8_lossy(p))
                                    .unwrap_or_default(),
                                change_user.database.unwrap_or_default(),
                            )
                            .await
                        {
                            Ok(()) => {
                                writers::write_ok_packet(
                                    &mut self.conn,
                                    0,
                                    0,
                                    StatusFlags::empty(),
                                )
                                .await?;
                            }
                            Err(_) => {
                                writers::write_err(
                                    ErrorKind::ER_ACCESS_DENIED_ERROR,
                                    format!("Access denied for user {}", username).as_bytes(),
                                    &mut self.conn,
                                )
                                .await?;
                            }
                        }
                    } else {
                        debug!("Received incorrect password");
                        writers::write_err(
                            ErrorKind::ER_ACCESS_DENIED_ERROR,
                            format!("Access denied for user {}", username).as_bytes(),
                            &mut self.conn,
                        )
                        .await?;
                    }
                    self.conn.flush().await?;
                }
                Command::Query(q) => {
                    let w = QueryResultWriter::new(&mut self.conn, false);
                    let res = self
                        .shim
                        .on_query(
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )
                        .await;

                    match res {
                        QueryResultsResponse::Command(cmd) => {
                            match cmd {
                                ParsedCommand::Deallocate(dealloc_id) => {
                                    if DeallocateId::All == dealloc_id {
                                        // mysql doesn't allow 'deallocate all',
                                        // should probably be a nom error.
                                        writers::write_err(
                                            ErrorKind::ER_PARSE_ERROR,
                                            "Unsupported 'DEALLOCATE PREPARE ALL'".as_bytes(),
                                            &mut self.conn,
                                        )
                                        .await?;
                                    } else {
                                        self.shim.on_close(dealloc_id.clone()).await;
                                        if let DeallocateId::Numeric(id) = dealloc_id {
                                            stmts.remove(&id);
                                            self.schema_cache.remove(&id);
                                        }
                                        writers::write_ok_packet(
                                            &mut self.conn,
                                            0,
                                            0,
                                            StatusFlags::empty(),
                                        )
                                        .await?;
                                    }
                                }
                            }
                        }
                        QueryResultsResponse::IoResult(result) => result?,
                    }
                }
                Command::Prepare(q) => {
                    let w = StatementMetaWriter {
                        conn: &mut self.conn,
                        stmts: &mut stmts,
                    };
                    self.shim
                        .on_prepare(
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                            &mut self.schema_cache,
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
                    writers::write_ok_packet(&mut self.conn, 0, 0, StatusFlags::empty()).await?;
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
                        let w = QueryResultWriter::new(&mut self.conn, true);
                        self.shim
                            .on_execute(stmt, params, w, &mut self.schema_cache)
                            .await?;
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
                    self.shim.on_close(DeallocateId::Numeric(stmt)).await;
                    stmts.remove(&stmt);
                    self.schema_cache.remove(&stmt);
                    // NOTE: spec dictates no response from server
                }
                Command::ListFields(_) => {
                    // This was deprecated in MySQL 5.7.11, but is still used by the `mysql` cli
                    // utility, for autocompletion/"auto-rehash" (`\rehash` will also manually
                    // trigger it)
                    writers::write_err(
                        ErrorKind::ER_UNKNOWN_COM_ERROR,
                        "COM_FIELD_LIST is unsupported".as_bytes(),
                        &mut self.conn,
                    )
                    .await?;
                }
                Command::Init(schema) => {
                    debug!(schema = %String::from_utf8_lossy(schema), "Handling COM_INIT_DB");
                    let w = InitWriter {
                        conn: &mut self.conn,
                    };
                    self.shim
                        .on_init(
                            ::std::str::from_utf8(schema)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            Some(w),
                        )
                        .await?;
                }
                Command::Ping => {
                    self.shim.on_ping().await?;
                    writers::write_ok_packet(&mut self.conn, 0, 0, StatusFlags::empty()).await?;
                    self.conn.flush().await?;
                }
                Command::ComSetOption(_) => {
                    // Readyset already has multi-statement support for the MySQL protocol, so
                    // we can simply respond with ok. We parse an incoming query as multiple single
                    // statements, so failure with any one will be forwarded to the underlying
                    // database as a single statement, meaning that the underlying database does
                    // not need to have multi-statement support enabled for this connection.
                    writers::write_ok_packet(&mut self.conn, 0, 0, StatusFlags::empty()).await?;
                    self.conn.flush().await?;
                }
                Command::Reset => {
                    self.shim.on_reset().await?;
                    writers::write_ok_packet(&mut self.conn, 0, 0, StatusFlags::empty()).await?;
                    self.conn.flush().await?;
                }
                Command::Quit => {
                    break;
                }
            }

            self.conn.flush().await?;
        }

        Ok(())
    }
}
