use nom::branch::alt;
use nom::bytes::complete::{tag, take, take_until};
use nom::combinator::{map, map_res, opt, rest};
use nom::error::FromExternalError;
use nom::number::complete::{le_i16, le_i24, le_i64, le_u16, le_u32, le_u8};
use nom::sequence::preceded;
use nom::IResult;

use crate::myc::constants::{CapabilityFlags, Command as CommandByte};

#[derive(Debug)]
pub struct ClientHandshake<'a> {
    pub capabilities: CapabilityFlags,
    pub maxps: u32,
    pub charset: u16,
    pub username: &'a str,
    pub password: &'a [u8],
    pub database: Option<&'a str>,
    pub auth_plugin_name: Option<&'a str>,
}

/// Parse a "length-encoded integer" as specified by the [mysql binary protocol documentation][docs]
///
/// [docs]: https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
fn lenenc_int(i: &[u8]) -> IResult<&[u8], i64> {
    let (i, first_byte) = le_u8(i)?;
    match first_byte {
        b @ 0x00..=0xfb => Ok((i, b.into())),
        0xfc => le_i16(i).map(|(i, n)| (i, n.into())),
        0xfd => le_i24(i).map(|(i, n)| (i, n.into())),
        0xfe => le_i64(i),
        0xff => Err(nom::Err::Error(nom::error::Error::new(
            i,
            nom::error::ErrorKind::Tag,
        ))),
    }
}

fn parse_bytes_to_string(i: &[u8]) -> Result<&str, nom::Err<nom::error::Error<&[u8]>>> {
    std::str::from_utf8(i).map_err(|e| {
        nom::Err::Error(nom::error::Error::from_external_error(
            i,
            nom::error::ErrorKind::Verify,
            e,
        ))
    })
}

fn null_terminated_string(i: &[u8]) -> IResult<&[u8], &str> {
    let (i, res) = map_res(take_until(&b"\0"[..]), parse_bytes_to_string)(i)?;
    let (i, _) = take(1u8)(i)?;
    Ok((i, res))
}

/// <https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41>
pub fn client_handshake(i: &[u8]) -> IResult<&[u8], ClientHandshake<'_>> {
    let (i, capabilities) = map(le_u32, CapabilityFlags::from_bits_truncate)(i)?;
    let (i, maxps) = le_u32(i)?;
    let (i, charset) = le_u8(i)?;
    let (i, _) = take(23u8)(i)?;
    let (i, username) = null_terminated_string(i)?;
    let (i, password) =
        if capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            let (i, auth_token_length) = lenenc_int(i)?;
            take(auth_token_length as usize)(i)?
        } else if capabilities.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
            let (i, auth_token_length) = le_u8(i)?;
            take(auth_token_length)(i)?
        } else {
            map(null_terminated_string, |s| s.as_bytes())(i)?
        };

    let (i, database) = if capabilities.contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB) {
        map(null_terminated_string, Some)(i)?
    } else {
        (i, None)
    };

    let (i, auth_plugin_name) = if capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
        opt(null_terminated_string)(i)?
    } else {
        (i, None)
    };

    Ok((
        i,
        ClientHandshake {
            capabilities,
            maxps,
            charset: charset.into(),
            username,
            password,
            database,
            auth_plugin_name,
        },
    ))
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Query(&'a [u8]),
    ListFields(&'a [u8]),
    Close(u32),
    ResetStmtData(u32),
    Prepare(&'a [u8]),
    Init(&'a [u8]),
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
}

pub fn execute(i: &[u8]) -> IResult<&[u8], Command<'_>> {
    let (i, stmt) = le_u32(i)?;
    let (i, _flags) = take(1u8)(i)?;
    let (i, _iterations) = le_u32(i)?;
    Ok((&[], Command::Execute { stmt, params: i }))
}

pub fn send_long_data(i: &[u8]) -> IResult<&[u8], Command<'_>> {
    let (i, stmt) = le_u32(i)?;
    let (i, param) = le_u16(i)?;
    Ok((
        &[],
        Command::SendLongData {
            stmt,
            param,
            data: i,
        },
    ))
}

pub fn parse(i: &[u8]) -> IResult<&[u8], Command<'_>> {
    alt((
        map(
            preceded(tag(&[CommandByte::COM_QUERY as u8]), rest),
            Command::Query,
        ),
        map(
            preceded(tag(&[CommandByte::COM_FIELD_LIST as u8]), rest),
            Command::ListFields,
        ),
        map(
            preceded(tag(&[CommandByte::COM_INIT_DB as u8]), rest),
            Command::Init,
        ),
        map(
            preceded(tag(&[CommandByte::COM_STMT_PREPARE as u8]), rest),
            Command::Prepare,
        ),
        map(
            preceded(tag(&[CommandByte::COM_STMT_RESET as u8]), le_u32),
            Command::ResetStmtData,
        ),
        preceded(tag(&[CommandByte::COM_STMT_EXECUTE as u8]), execute),
        preceded(
            tag(&[CommandByte::COM_STMT_SEND_LONG_DATA as u8]),
            send_long_data,
        ),
        map(
            preceded(tag(&[CommandByte::COM_STMT_CLOSE as u8]), le_u32),
            Command::Close,
        ),
        map(tag(&[CommandByte::COM_QUIT as u8]), |_| Command::Quit),
        map(tag(&[CommandByte::COM_PING as u8]), |_| Command::Ping),
    ))(i)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::myc::constants::{CapabilityFlags, UTF8_GENERAL_CI};
    use crate::packet::PacketReader;

    #[tokio::test]
    async fn it_parses_handshake() {
        let data = &[
            0x25, 0x00, 0x00, 0x01, 0x85, 0xa6, 0x3f, 0x20, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x6f, 0x6e, 0x00, 0x00,
        ];
        let r = Cursor::new(&data[..]);
        let mut pr = PacketReader::new(r);
        let (_, p) = pr.next().await.unwrap().unwrap();
        let (_, handshake) = client_handshake(&p).unwrap();
        println!("{:?}", handshake);
        assert!(handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_LONG_PASSWORD));
        assert!(handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_MULTI_RESULTS));
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB));
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF));
        assert_eq!(handshake.charset, UTF8_GENERAL_CI);
        assert_eq!(handshake.username, "jon");
        assert_eq!(handshake.maxps, 16777216);
    }

    #[tokio::test]
    async fn it_parses_request() {
        let data = &[
            0x21, 0x00, 0x00, 0x00, 0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e,
            0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
        ];
        let r = Cursor::new(&data[..]);
        let mut pr = PacketReader::new(r);
        let (_, p) = pr.next().await.unwrap().unwrap();
        let (_, cmd) = parse(&p).unwrap();
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
        let data = &[
            0x21, 0x00, 0x00, 0x00, 0x04, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e,
            0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
        ];
        let r = Cursor::new(&data[..]);
        let mut pr = PacketReader::new(r);
        let (_, p) = pr.next().await.unwrap().unwrap();
        let (_, cmd) = parse(&p).unwrap();
        assert_eq!(
            cmd,
            Command::ListFields(&b"select @@version_comment limit 1"[..])
        );
    }
}
