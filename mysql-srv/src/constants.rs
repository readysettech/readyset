#![allow(dead_code)]
// Manually extracted from mysql-5.5.23/include/mysql_com.h

/// new more secure passwords
pub const LONG_PASSWORD: u32 = 0x00000001;

/// found instead of affected rows
pub const FOUND_ROWS: u32 = 0x00000002;
/// get all column flags
pub const LONG_FLAG: u32 = 0x00000004;
/// one can specify db on connect
pub const CONNECT_WITH_DB: u32 = 0x00000008;
/// don't allow database.table.column
pub const NO_SCHEMA: u32 = 0x00000010;
/// can use compression protocol
pub const COMPRESS: u32 = 0x00000020;
/// odbc client
pub const ODBC: u32 = 0x00000040;
/// can use LOAD DATA LOCAL
pub const LOCAL_FILES: u32 = 0x00000080;
/// ignore spaces before ''
pub const IGNORE_SPACE: u32 = 0x00000100;
/// new 4.1 protocol
pub const PROTOCOL_41: u32 = 0x00000200;
/// this is an interactive client
pub const INTERACTIVE: u32 = 0x00000400;
/// switch to ssl after handshake
pub const SSL: u32 = 0x00000800;
/// IGNORE sigpipes
pub const IGNORE_SIGPIPE: u32 = 0x00001000;
/// client knows about transactions
pub const TRANSACTIONS: u32 = 0x00002000;
/// old flag for 4.1 protocol
pub const RESERVED: u32 = 0x00004000;
/// new 4.1 authentication
pub const SECURE_CONNECTION: u32 = 0x00008000;
/// enable/disable multi-stmt support
pub const MULTI_STATEMENTS: u32 = 0x00010000;
/// enable/disable multi-results
pub const MULTI_RESULTS: u32 = 0x00020000;
/// multi-results in ps-protocol
pub const PS_MULTI_RESULTS: u32 = 0x00040000;
/// client supports plugin authentication
pub const PLUGIN_AUTH: u32 = 0x00080000;
/// permits connection attributes
pub const CONNECT_ATTRS: u32 = 0x00100000;
/// Understands length-encoded integer for auth response data in Protocol::HandshakeResponse41.
pub const PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;
/// Announces support for expired password extension.
pub const CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 0x00400000;
/// Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data
/// after a OK packet.
pub const SESSION_TRACK: u32 = 0x00800000;
/// Can send OK after a Text Resultset.
pub const DEPRECATE_EOF: u32 = 0x01000000;
/// Client supports plugin authentication
pub const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;

pub const SSL_VERIFY_SERVER_CERT: u32 = 0x40000000;
pub const REMEMBER_OPTIONS: u32 = 0x80000000;
