use byteorder::{ByteOrder, LittleEndian};
use std::io;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const U24_MAX: usize = 16_777_215;

pub struct PacketWriter<W> {
    to_write: Vec<u8>,
    bytes_written: usize,
    seq: u8,
    w: W,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for PacketWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, tokio::io::Error>> {
        use std::cmp::min;
        if self.to_write.len() == U24_MAX {
            match self.as_mut().poll_end_packet(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(x) => x?,
            }
        }
        let left = min(buf.len(), U24_MAX - self.to_write.len());
        self.to_write.extend(&buf[..left]);
        Poll::Ready(Ok(left))
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), tokio::io::Error>> {
        match self.as_mut().poll_end_packet(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(x) => x?,
        }
        match self.as_mut().project_writer().poll_flush(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(x) => x?,
        }
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), tokio::io::Error>> {
        self.project_writer().poll_shutdown(cx)
    }
}

impl<W: AsyncWrite + Unpin> PacketWriter<W> {
    fn project_writer(self: Pin<&mut Self>) -> Pin<&mut W> {
        // SAFETY: W is Unpin anyway, so pinning doesn't matter
        unsafe { self.map_unchecked_mut(|s| &mut s.w) }
    }
    pub fn new(w: W) -> Self {
        PacketWriter {
            to_write: vec![0, 0, 0, 0],
            bytes_written: 0,
            seq: 0,
            w,
        }
    }

    fn poll_end_packet(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), tokio::io::Error>> {
        let len = self.to_write.len() - 4;
        if len != 0 || self.bytes_written > 0 {
            if self.bytes_written == 0 {
                LittleEndian::write_u24(&mut self.to_write[0..3], len as u32);
                self.to_write[3] = self.seq;
            }
            let s = Pin::into_inner(self);
            loop {
                let written =
                    match Pin::new(&mut s.w).poll_write(cx, &s.to_write[s.bytes_written..]) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(x) => x?,
                    };
                s.bytes_written += written;
                if s.bytes_written < s.to_write.len() {
                    continue;
                }
                s.bytes_written = 0;
                s.seq = s.seq.wrapping_add(1);
                s.to_write.truncate(4); // back to just header
                break;
            }
        }
        Poll::Ready(Ok(()))
    }

    pub async fn write_buf_and_flush(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write_all(buf).await?;
        self.flush().await?;
        Ok(())
    }

    pub async fn end_packet(&mut self) -> io::Result<()> {
        self.flush().await?;
        Ok(())
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

impl<R: AsyncRead + Unpin> PacketReader<R> {
    pub async fn next(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        self.start = self.bytes.len() - self.remaining;

        loop {
            if self.remaining != 0 {
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
            self.bytes.resize(std::cmp::max(4096, end * 2), 0);
            let read = {
                let mut buf = &mut self.bytes[end..];
                self.r.read(&mut buf).await?
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

pub fn fullpacket(i: &[u8]) -> nom::IResult<&[u8], (u8, &[u8])> {
    let (i, _) = nom::bytes::complete::tag(&[0xff, 0xff, 0xff])(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(U24_MAX)(i)?;
    Ok((i, (seq[0], bytes)))
}

pub fn onepacket(i: &[u8]) -> nom::IResult<&[u8], (u8, &[u8])> {
    let (i, length) = nom::number::complete::le_u24(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(length)(i)?;
    Ok((i, (seq[0], bytes)))
}

// Clone because of https://github.com/Geal/nom/issues/1008
#[derive(Clone)]
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
            assert!(self.1.is_empty());
            let mut v = self.0.to_vec();
            v.extend(bytes);
            self.1 = v;
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
use std::pin::Pin;

impl<'a> Deref for Packet<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

fn packet(i: &[u8]) -> nom::IResult<&[u8], (u8, Packet<'_>)> {
    nom::combinator::map(
        nom::sequence::pair(
            nom::multi::fold_many0(
                fullpacket,
                (0, None),
                |(seq, pkt): (_, Option<Packet<'_>>), (nseq, p)| {
                    let pkt = if let Some(mut pkt) = pkt {
                        assert_eq!(nseq, seq + 1);
                        pkt.extend(p);
                        Some(pkt)
                    } else {
                        Some(Packet(p, Vec::new()))
                    };
                    (nseq, pkt)
                },
            ),
            onepacket,
        ),
        move |(full, last)| {
            let seq = last.0;
            let pkt = if let Some(mut pkt) = full.1 {
                assert_eq!(last.0, full.0 + 1);
                pkt.extend(last.1);
                pkt
            } else {
                Packet(last.1, Vec::new())
            };
            (seq, pkt)
        },
    )(i)
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
