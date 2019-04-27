use byteorder::{ByteOrder, LittleEndian};
use nom::{self, IResult};
use std::io;
use std::io::prelude::*;

const U24_MAX: usize = 16_777_215;

pub struct PacketWriter<W> {
    to_write: Vec<u8>,
    seq: u8,
    w: W,
}

impl<W: Write> Write for PacketWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        use std::cmp::min;
        let left = min(buf.len(), U24_MAX - self.to_write.len());
        self.to_write.extend(&buf[..left]);

        if self.to_write.len() == U24_MAX {
            self.end_packet()?;
        }
        Ok(left)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.maybe_end_packet()?;
        self.w.flush()
    }
}

impl<W: Write> PacketWriter<W> {
    pub fn new(w: W) -> Self {
        PacketWriter {
            to_write: vec![0, 0, 0, 0],
            seq: 0,
            w,
        }
    }

    fn maybe_end_packet(&mut self) -> io::Result<()> {
        let len = self.to_write.len() - 4;
        if len != 0 {
            LittleEndian::write_u24(&mut self.to_write[0..3], len as u32);
            self.to_write[3] = self.seq;
            self.seq = self.seq.wrapping_add(1);

            self.w.write_all(&self.to_write[..])?;
            self.to_write.truncate(4); // back to just header
        }
        Ok(())
    }

    pub fn end_packet(&mut self) -> io::Result<()> {
        self.maybe_end_packet()
    }
}

impl<W> PacketWriter<W> {
    pub fn set_seq(&mut self, seq: u8) {
        self.seq = seq;
    }
}

pub struct PacketReader<R> {
    bytes: Vec<u8>,
    start: usize,
    remaining: usize,
    r: R,
}

impl<R> PacketReader<R> {
    pub fn new(r: R) -> Self {
        PacketReader {
            bytes: Vec::new(),
            start: 0,
            remaining: 0,
            r,
        }
    }
}

impl<R: Read> PacketReader<R> {
    pub fn next(&mut self) -> io::Result<Option<(u8, Packet)>> {
        self.start = self.bytes.len() - self.remaining;

        loop {
            {
                let bytes = {
                    // NOTE: this is all sorts of unfortunate. what we really want to do is to give
                    // &self.bytes[self.start..] to `packet()`, and the lifetimes should all work
                    // out. however, without NLL, borrowck doesn't realize that self.bytes is no
                    // longer borrowed after the match, and so can be mutated.
                    let bytes = &self.bytes[self.start..];
                    unsafe { ::std::slice::from_raw_parts(bytes.as_ptr(), bytes.len()) }
                };
                match packet(bytes) {
                    Ok((rest, p)) => {
                        self.remaining = rest.len();
                        return Ok(Some(p));
                    }
                    Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {}
                    Err(nom::Err::Failure(ctx)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{:?}", ctx),
                        ))
                    }
                }
            }

            // we need to read some more
            self.bytes.drain(0..self.start);
            self.start = 0;
            let end = self.bytes.len();
            self.bytes.resize(end + 1024, 0);
            let read = {
                let mut buf = &mut self.bytes[end..];
                self.r.read(&mut buf)?
            };
            self.bytes.truncate(end + read);
            self.remaining = self.bytes.len();

            if read == 0 {
                if self.bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{} unhandled bytes", self.bytes.len()),
                    ));
                }
            }
        }
    }
}

named!(
    fullpacket<(u8, &[u8])>,
    do_parse!(
        tag!(&[0xff, 0xff, 0xff]) >> seq: take!(1) >> bytes: take!(U24_MAX) >> (seq[0], bytes)
    )
);

named!(
    onepacket<(u8, &[u8])>,
    do_parse!(
        length: apply!(nom::le_u24,) >> seq: take!(1) >> bytes: take!(length) >> (seq[0], bytes)
    )
);

pub struct Packet<'a>(&'a [u8], Vec<u8>);

impl<'a> Packet<'a> {
    fn extend(&mut self, bytes: &'a [u8]) {
        if self.0.is_empty() {
            if self.1.is_empty() {
                // first extend
                self.0 = bytes;
            } else {
                // later extend
                self.1.extend(bytes);
            }
        } else {
            use std::mem;

            assert!(self.1.is_empty());
            let mut v = self.0.to_vec();
            v.extend(bytes);
            mem::replace(&mut self.1, v);
            self.0 = &[];
        }
    }
}

impl<'a> AsRef<[u8]> for Packet<'a> {
    fn as_ref(&self) -> &[u8] {
        if self.1.is_empty() {
            self.0
        } else {
            &*self.1
        }
    }
}

use std::ops::Deref;
impl<'a> Deref for Packet<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

fn packet<'a>(input: &'a [u8]) -> IResult<&'a [u8], (u8, Packet<'a>)> {
    do_parse!(
        input,
        full: fold_many0!(
            fullpacket,
            (0, None),
            |(seq, pkt): (_, Option<Packet<'a>>), (nseq, p)| {
                let pkt = if let Some(mut pkt) = pkt {
                    assert_eq!(nseq, seq + 1);
                    pkt.extend(p);
                    Some(pkt)
                } else {
                    Some(Packet(p, Vec::new()))
                };
                (nseq, pkt)
            }
        ) >> last: onepacket
            >> ({
                let seq = last.0;
                let pkt = if let Some(mut pkt) = full.1 {
                    assert_eq!(last.0, full.0 + 1);
                    pkt.extend(last.1);
                    pkt
                } else {
                    Packet(last.1, Vec::new())
                };
                (seq, pkt)
            })
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_ping() {
        assert_eq!(
            onepacket(&[0x01, 0, 0, 0, 0x10]).unwrap().1,
            (0, &[0x10][..])
        );
    }

    #[test]
    fn test_ping() {
        let p = packet(&[0x01, 0, 0, 0, 0x10]).unwrap().1;
        assert_eq!(p.0, 0);
        assert_eq!(&*p.1, &[0x10][..]);
    }

    #[test]
    fn test_long_exact() {
        let mut data = Vec::new();
        data.push(0xff);
        data.push(0xff);
        data.push(0xff);
        data.push(0);
        data.extend(&[0; U24_MAX][..]);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(1);

        let (rest, p) = packet(&data[..]).unwrap();
        assert!(rest.is_empty());
        assert_eq!(p.0, 1);
        assert_eq!(p.1.len(), U24_MAX);
        assert_eq!(&*p.1, &[0; U24_MAX][..]);
    }

    #[test]
    fn test_long_more() {
        let mut data = Vec::new();
        data.push(0xff);
        data.push(0xff);
        data.push(0xff);
        data.push(0);
        data.extend(&[0; U24_MAX][..]);
        data.push(0x01);
        data.push(0x00);
        data.push(0x00);
        data.push(1);
        data.push(0x10);

        let (rest, p) = packet(&data[..]).unwrap();
        assert!(rest.is_empty());
        assert_eq!(p.0, 1);
        assert_eq!(p.1.len(), U24_MAX + 1);
        assert_eq!(&p.1[..U24_MAX], &[0; U24_MAX][..]);
        assert_eq!(&p.1[U24_MAX..], &[0x10]);
    }
}
