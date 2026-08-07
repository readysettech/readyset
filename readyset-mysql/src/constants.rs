/// The default collation id to use when writing out column packets, matching the collation
/// advertised in the handshake greeting.
pub static DEFAULT_COLLATION: u16 = mysql_srv::DEFAULT_HANDSHAKE_COLLATION as u16;

pub static DEFAULT_COLLATION_NUMERIC: u16 = mysql_common::collations::CollationId::BINARY as u16;
