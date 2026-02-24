use std::collections::HashMap;
use std::io;
use std::str;

use crate::myc::constants::{CapabilityFlags, Command as CommandByte, UTF8MB4_GENERAL_CI};

#[derive(Debug)]
pub struct ClientHandshake<'a> {
    pub capabilities: CapabilityFlags,
    #[allow(dead_code)]
    pub maxps: u32,
    #[allow(dead_code)]
    pub charset: u16,
    pub username: &'a str,
    pub password: &'a [u8],
    pub database: Option<&'a str>,
    pub auth_plugin_name: Option<&'a str>,
    #[allow(dead_code)]
    pub connect_attrs: HashMap<&'a str, &'a str>,
}

#[derive(Debug)]
pub struct ClientChangeUser<'a> {
    pub username: &'a str,
    pub password: &'a [u8],
    pub database: Option<&'a str>,
    #[allow(dead_code)]
    pub charset: u16,
    pub auth_plugin_name: &'a str,
}

struct PacketParser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PacketParser<'a> {
    fn new(data: &'a [u8]) -> Self {
        PacketParser { data, pos: 0 }
    }

    fn remaining(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        if self.pos >= self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected end of packet",
            ));
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_u16_le(&mut self) -> io::Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32_le(&mut self) -> io::Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_i16_le(&mut self) -> io::Result<i16> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_i24_le(&mut self) -> io::Result<i32> {
        let bytes = self.read_bytes(3)?;
        let n = (bytes[0] as u32) | ((bytes[1] as u32) << 8) | ((bytes[2] as u32) << 16);
        // Sign-extend from 24 bits to 32 bits
        Ok(if n & 0x80_0000 != 0 {
            (n | 0xFF00_0000) as i32
        } else {
            n as i32
        })
    }

    fn read_i64_le(&mut self) -> io::Result<i64> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected end of packet",
            ));
        }
        let bytes = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    fn skip(&mut self, n: usize) -> io::Result<()> {
        if self.pos + n > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected end of packet",
            ));
        }
        self.pos += n;
        Ok(())
    }

    /// Parse a "length-encoded integer" as specified by the
    /// [mysql binary protocol documentation][docs]
    ///
    /// [docs]: https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
    fn read_lenenc_int(&mut self) -> io::Result<i64> {
        let first = self.read_u8()?;
        match first {
            b @ 0x00..=0xfb => Ok(b.into()),
            0xfc => self.read_i16_le().map(Into::into),
            0xfd => self.read_i24_le().map(Into::into),
            0xfe => self.read_i64_le(),
            0xff => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid length-encoded integer prefix 0xff",
            )),
        }
    }

    /// Parse a length-encoded string as specified by the
    /// [mysql binary protocol documentation][docs]
    ///
    /// [docs]: https://dev.mysql.com/doc/internals/en/string.html#length-encoded-string
    fn read_lenenc_str(&mut self) -> io::Result<&'a str> {
        let length = self.read_lenenc_int()? as usize;
        let bytes = self.read_bytes(length)?;
        str::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn read_null_str(&mut self) -> io::Result<&'a str> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "missing null terminator",
            ));
        }
        let bytes = &self.data[start..self.pos];
        self.pos += 1; // skip the null byte
        str::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// Parse a COM_CHANGE_USER packet as specified by the [mysql binary protocol documentation][docs]
/// [docs]: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_change_user.html
pub fn change_user(
    i: &[u8],
    client_capability_flags: CapabilityFlags,
) -> io::Result<ClientChangeUser<'_>> {
    let mut p = PacketParser::new(i);

    let username = p.read_null_str()?;
    let password = if client_capability_flags.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
        let auth_token_length = p.read_u8()?;
        p.read_bytes(auth_token_length as usize)?
    } else {
        p.read_null_str()?.as_bytes()
    };
    let database = Some(p.read_null_str()?);
    let (charset, auth_plugin_name) = if !p.is_empty() {
        let charset = if client_capability_flags.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
            p.read_u16_le()?
        } else {
            UTF8MB4_GENERAL_CI
        };
        let auth_plugin_name =
            if client_capability_flags.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
                p.read_null_str()?
            } else {
                ""
            };
        (charset, auth_plugin_name)
    } else {
        (UTF8MB4_GENERAL_CI, "")
    };

    Ok(ClientChangeUser {
        username,
        password,
        database,
        charset,
        auth_plugin_name,
    })
}

pub fn is_ssl_request(i: &[u8]) -> io::Result<bool> {
    let mut p = PacketParser::new(i);
    let capabilities = CapabilityFlags::from_bits_truncate(p.read_u32_le()?);
    Ok(capabilities.contains(CapabilityFlags::CLIENT_SSL))
}

/// <https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41>
pub fn client_handshake(i: &[u8]) -> io::Result<ClientHandshake<'_>> {
    let mut p = PacketParser::new(i);

    let capabilities = CapabilityFlags::from_bits_truncate(p.read_u32_le()?);
    let maxps = p.read_u32_le()?;
    let charset = p.read_u8()? as u16;
    p.skip(23)?;
    let username = p.read_null_str()?;
    let password = if capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
    {
        let auth_token_length = p.read_lenenc_int()? as usize;
        p.read_bytes(auth_token_length)?
    } else if capabilities.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
        let auth_token_length = p.read_u8()?;
        p.read_bytes(auth_token_length as usize)?
    } else {
        p.read_null_str()?.as_bytes()
    };

    let database = if capabilities.contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB) {
        Some(p.read_null_str()?)
    } else {
        None
    };

    let auth_plugin_name = if capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
        // A client may omit the auth plugin name even with CLIENT_PLUGIN_AUTH set
        // (no null terminator present), so attempt to read and restore position on failure.
        let saved_pos = p.pos;
        match p.read_null_str() {
            Ok(s) => Some(s),
            Err(_) => {
                p.pos = saved_pos;
                None
            }
        }
    } else {
        None
    };

    let connect_attrs = if capabilities.contains(CapabilityFlags::CLIENT_CONNECT_ATTRS) {
        // If the flag is set, we should receive a length-encoded integer followed by a block of
        // key-value pairs. Adding a fallback for safety.
        let saved_pos = p.pos;
        let length = p.read_lenenc_int().unwrap_or_else(|_| {
            p.pos = saved_pos;
            0
        }) as usize;
        let attrs_data = p.read_bytes(length)?;
        let mut attrs_parser = PacketParser::new(attrs_data);
        let mut connect_attrs = HashMap::new();
        while !attrs_parser.is_empty() {
            let k = attrs_parser.read_lenenc_str()?;
            let v = attrs_parser.read_lenenc_str()?;
            connect_attrs.insert(k, v);
        }
        connect_attrs
    } else {
        HashMap::new()
    };

    Ok(ClientHandshake {
        capabilities,
        maxps,
        charset,
        username,
        password,
        database,
        auth_plugin_name,
        connect_attrs,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Query(&'a [u8]),
    ListFields(&'a [u8]),
    Close(u32),
    Reset,
    ResetStmtData(u32),
    Prepare(&'a [u8]),
    Init(&'a [u8]),
    ComSetOption(&'a [u8]),
    Execute {
        stmt: u32,
        params: &'a [u8],
    },
    SendLongData {
        stmt: u32,
        param: u16,
        data: &'a [u8],
    },
    Ping,
    Quit,
    ChangeUser(&'a [u8]),
}

fn parse_execute<'a>(p: &mut PacketParser<'a>) -> io::Result<Command<'a>> {
    let stmt = p.read_u32_le()?;
    p.skip(1)?; // flags
    p.skip(4)?; // iteration-count
    Ok(Command::Execute {
        stmt,
        params: p.remaining(),
    })
}

fn parse_send_long_data<'a>(p: &mut PacketParser<'a>) -> io::Result<Command<'a>> {
    let stmt = p.read_u32_le()?;
    let param = p.read_u16_le()?;
    Ok(Command::SendLongData {
        stmt,
        param,
        data: p.remaining(),
    })
}

pub fn parse(i: &[u8]) -> io::Result<Command<'_>> {
    let mut p = PacketParser::new(i);
    let cmd_byte = p.read_u8()?;
    match cmd_byte {
        b if b == CommandByte::COM_QUERY as u8 => Ok(Command::Query(p.remaining())),
        b if b == CommandByte::COM_FIELD_LIST as u8 => Ok(Command::ListFields(p.remaining())),
        b if b == CommandByte::COM_INIT_DB as u8 => Ok(Command::Init(p.remaining())),
        b if b == CommandByte::COM_SET_OPTION as u8 => Ok(Command::ComSetOption(p.remaining())),
        b if b == CommandByte::COM_STMT_PREPARE as u8 => Ok(Command::Prepare(p.remaining())),
        b if b == CommandByte::COM_STMT_RESET as u8 => Ok(Command::ResetStmtData(p.read_u32_le()?)),
        b if b == CommandByte::COM_STMT_EXECUTE as u8 => parse_execute(&mut p),
        b if b == CommandByte::COM_STMT_SEND_LONG_DATA as u8 => parse_send_long_data(&mut p),
        b if b == CommandByte::COM_STMT_CLOSE as u8 => Ok(Command::Close(p.read_u32_le()?)),
        b if b == CommandByte::COM_RESET_CONNECTION as u8 => Ok(Command::Reset),
        b if b == CommandByte::COM_QUIT as u8 => Ok(Command::Quit),
        b if b == CommandByte::COM_PING as u8 => Ok(Command::Ping),
        b if b == CommandByte::COM_CHANGE_USER as u8 => Ok(Command::ChangeUser(p.remaining())),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported MySQL command byte: 0x{cmd_byte:02x}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::myc::constants::{CapabilityFlags, UTF8_GENERAL_CI};
    use crate::packet::PacketConn;

    #[tokio::test]
    async fn it_detects_ssl_request() {
        let mut data = [
            0x20, 0x00, 0x00, 0x01, 0x05, 0xae, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01, 0x08, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let r: Cursor<&mut [u8]> = Cursor::new(&mut data[..]);
        let mut pr = PacketConn::new(r);
        let packet = pr.next().await.unwrap().unwrap();
        assert!(is_ssl_request(&packet.data).unwrap());
    }

    #[tokio::test]
    async fn it_parses_handshake() {
        let mut data = [
            0x39, 0x00, 0x00, 0x01, 0x85, 0xa6, 0x3f, 0x20, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x6f, 0x6e, 0x00, 0x00,
            // connection attributes length (lenenc int) + key/value pairs
            0x13, // total length = 19 bytes
            0x0d, // key length = 13
            0x5f, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x61, 0x6d, 0x5f, 0x6e, 0x61, 0x6d,
            0x65, // "_program_name"
            0x04, // value length = 4
            0x74, 0x65, 0x73, 0x74, // "test"
        ];
        let r: Cursor<&mut [u8]> = Cursor::new(&mut data[..]);
        let mut pr = PacketConn::new(r);
        let packet = pr.next().await.unwrap().unwrap();
        let handshake = client_handshake(&packet.data).unwrap();
        assert!(handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_LONG_PASSWORD));
        assert!(handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_MULTI_RESULTS));
        assert!(handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_CONNECT_ATTRS));
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB));
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF));
        assert_eq!(handshake.charset, UTF8_GENERAL_CI);
        assert_eq!(handshake.username, "jon");
        assert_eq!(handshake.maxps, 16777216);
        assert_eq!(handshake.connect_attrs.get("_program_name"), Some(&"test"));
    }

    #[tokio::test]
    async fn it_parses_request() {
        let mut data = [
            0x21, 0x00, 0x00, 0x00, 0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e,
            0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
        ];
        let r: Cursor<&mut [u8]> = Cursor::new(&mut data[..]);
        let mut pr = PacketConn::new(r);
        let packet = pr.next().await.unwrap().unwrap();
        let cmd = parse(&packet.data).unwrap();
        assert_eq!(
            cmd,
            Command::Query(&b"select @@version_comment limit 1"[..])
        );
    }

    #[tokio::test]
    async fn it_handles_list_fields() {
        // mysql_list_fields (CommandByte::COM_FIELD_LIST / 0x04) has been deprecated in mysql 5.7
        // and will be removed in a future version. The mysql command line tool issues one
        // of these commands after switching databases with USE <DB>.
        let mut data = [
            0x21, 0x00, 0x00, 0x00, 0x04, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e,
            0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
        ];
        let r: Cursor<&mut [u8]> = Cursor::new(&mut data[..]);
        let mut pr = PacketConn::new(r);
        let packet = pr.next().await.unwrap().unwrap();
        let cmd = parse(&packet.data).unwrap();
        assert_eq!(
            cmd,
            Command::ListFields(&b"select @@version_comment limit 1"[..])
        );
    }

    #[tokio::test]
    async fn it_parses_change_user() {
        let mut data = [
            0x24, 0x00, 0x00, 0x00, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x00, 0x74, 0x65, 0x73, 0x74,
            0x00, 0x2d, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76,
            0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00, 0x00,
        ];
        let r: Cursor<&mut [u8]> = Cursor::new(&mut data[..]);
        let mut pr = PacketConn::new(r);
        let packet = pr.next().await.unwrap().unwrap();
        let capability_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH;
        let changeuser = change_user(&packet.data, capability_flags).unwrap();
        assert_eq!(changeuser.username, "root");
        assert_eq!(changeuser.password, b"");
        assert_eq!(changeuser.database, Some("test"));
        assert_eq!(changeuser.charset, UTF8MB4_GENERAL_CI);
        assert_eq!(changeuser.auth_plugin_name, "mysql_native_password");

        let mut data = [
            0x38, 0x00, 0x00, 0x00, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x14, 0x95, 0x16, 0xb1, 0x01,
            0x40, 0x6d, 0x7c, 0xc3, 0x17, 0x22, 0xc5, 0x9d, 0x00, 0xf3, 0x5d, 0x37, 0xb9, 0xb5,
            0x6d, 0x0f, 0x74, 0x65, 0x73, 0x74, 0x00, 0x2d, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c,
            0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f,
            0x72, 0x64, 0x00, 0x00,
        ];
        let r: Cursor<&mut [u8]> = Cursor::new(&mut data[..]);
        let mut pr = PacketConn::new(r);
        let packet = pr.next().await.unwrap().unwrap();
        let changeuser = change_user(&packet.data, capability_flags).unwrap();
        assert_eq!(changeuser.username, "root");
        assert_eq!(
            changeuser.password,
            &[
                0x95, 0x16, 0xb1, 0x01, 0x40, 0x6d, 0x7c, 0xc3, 0x17, 0x22, 0xc5, 0x9d, 0x00, 0xf3,
                0x5d, 0x37, 0xb9, 0xb5, 0x6d, 0x0f
            ]
        );
        assert_eq!(changeuser.database, Some("test"));
        assert_eq!(changeuser.charset, UTF8MB4_GENERAL_CI);
        assert_eq!(changeuser.auth_plugin_name, "mysql_native_password");
    }
}
