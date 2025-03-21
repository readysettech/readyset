/// The default character set to use when writing out column packets.
pub static DEFAULT_CHARACTER_SET: u16 =
    mysql_common::collations::CollationId::UTF8MB4_GENERAL_CI as u16;

pub static DEFAULT_CHARACTER_SET_NUMERIC: u16 =
    mysql_common::collations::CollationId::BINARY as u16;
