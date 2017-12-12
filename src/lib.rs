extern crate mysql;
#[macro_use]
extern crate nom;

use mysql::consts::{CapabilityFlags, Command as CommandByte};

mod packet;

pub use packet::packet;

#[derive(Debug)]
pub struct ClientHandshake<'a> {
    capabilities: mysql::consts::CapabilityFlags,
    maxps: u32,
    collation: u16,
    username: &'a [u8],
}

named!(
    pub client_handshake<ClientHandshake>,
    do_parse!(
        cap: apply!(nom::le_u32,) >>
        maxps: apply!(nom::le_u32,) >>
        collation: take!(1) >>
        take!(23) >>
        username: take_until!(&b"\0"[..]) >>
        tag!(b"\0") >> //rustfmt
        (ClientHandshake {
            capabilities: CapabilityFlags::from_bits_truncate(cap),
            maxps,
            collation: u16::from(collation[0]),
            username,
        })
    )
);

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Query(&'a [u8]),
    Init(&'a [u8]),
}

named!(
    pub command<Command>,
    alt!(
        preceded!(tag!(&[CommandByte::COM_QUERY as u8]), apply!(nom::rest,)) => { |sql| Command::Query(sql) } |
        preceded!(tag!(&[CommandByte::COM_INIT_DB as u8]), apply!(nom::rest,)) => { |db| Command::Init(db) }
    )
);

named!(
    pub lenencint<Option<i64>>,
    alt!(
        tag!(&[0xFB]) => { |_| None } |
        preceded!(tag!(&[0xFC]), apply!(nom::le_i16,)) => { |i| Some(i64::from(i)) } |
        preceded!(tag!(&[0xFD]), apply!(nom::le_i24,)) => { |i| Some(i64::from(i)) } |
        preceded!(tag!(&[0xFC]), apply!(nom::le_i64,)) => { |i| Some(i) } |
        take!(1) => { |i: &[u8]| Some(i64::from(i[0])) }
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use mysql::consts::{CapabilityFlags, UTF8_GENERAL_CI};

    #[test]
    fn it_parses_handshake() {
        let data = &[
            0x25, 0x00, 0x00, 0x01, 0x85, 0xa6, 0x3f, 0x20, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x6f, 0x6e, 0x00, 0x00,
        ];
        let (_, p) = packet(&data[..]).unwrap();
        let (_, handshake) = client_handshake(&p.1).unwrap();
        println!("{:?}", handshake);
        assert!(
            handshake
                .capabilities
                .contains(CapabilityFlags::CLIENT_LONG_PASSWORD)
        );
        assert!(
            handshake
                .capabilities
                .contains(CapabilityFlags::CLIENT_MULTI_RESULTS)
        );
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB));
        assert!(!handshake
            .capabilities
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF));
        assert_eq!(handshake.collation, UTF8_GENERAL_CI);
        assert_eq!(handshake.username, &b"jon"[..]);
        assert_eq!(handshake.maxps, 16777216);
    }

    #[test]
    fn it_parses_request() {
        let data = &[
            0x21, 0x00, 0x00, 0x00, 0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40,
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e,
            0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
        ];
        let (_, p) = packet(&data[..]).unwrap();
        let (_, cmd) = command(&p.1).unwrap();
        assert_eq!(
            cmd,
            Command::Query(&b"select @@version_comment limit 1"[..])
        );
    }
}
