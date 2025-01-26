use std::io::{self, IoSlice};
use std::ops::Deref;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::resultset::MAX_POOL_ROW_CAPACITY;

use crate::tls::SwitchableStream;

/// The maximum size of data we can be sent in a single mysql packet.
///
/// The limit is 24 bits as the mysql header protocol uses 24 bits to encode the length of the packet.
/// If you have a row that is larger than this, it needs to be split up into multiple packets.
///
/// Note that this is slightly different than the mysql variable `max_allowed_packet` [0],
/// as that is the max size of a given row (which might be chunked over several packets).
///
/// [0] https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_max_allowed_packet
pub const MAX_PACKET_CHUNK_SIZE: usize = 16_777_215;

const MAX_POOL_BUFFERS: usize = 64;

pub struct PacketConn<S: AsyncRead + AsyncWrite + Unpin> {
    // read variables
    // A buffer to hold incoming socket bytes, while building up for a complete packet.
    buffer: BytesMut,

    // write variables
    pub seq: u8,
    queue: Vec<QueuedPacket>,
    /// Reusable packets
    preallocated: Vec<QueuedPacket>,

    pub stream: SwitchableStream<S>,
}

/// Type for packets being enqueued in the packet writer.
enum QueuedPacket {
    /// Raw, reference counted queued packets are written as-is; these packets include header chunks.
    Raw(Arc<[u8]>),
    /// Raw, non-reference-counted queued packets are written as-is; these packets include header chunks.
    Plain(Vec<u8>),
    /// Packets which do not include their headers are written as two IoSlices:
    /// one for the header and one for the body.
    WithHeader([u8; 4], Vec<u8>),
    /// Packets which are larger than MAX_PACKET_CHUNK_SIZE are split into multiple packets.
    /// The `data` field contains the actual packet data _no header bytes_, and the `headers`
    /// field contains the packet headers.
    LargePacket {
        headers: Vec<[u8; 4]>,
        data: Vec<u8>,
    },
}

impl QueuedPacket {
    // Should this packet be reused
    fn poolable(&self) -> bool {
        !matches!(self, QueuedPacket::Raw(_))
    }

    fn len(&self) -> usize {
        match self {
            QueuedPacket::Raw(r) => r.len(),
            QueuedPacket::Plain(p) => p.len(),
            QueuedPacket::WithHeader(_, p) => p.len(),
            QueuedPacket::LargePacket {
                headers: _,
                data: p,
            } => p.len(),
        }
    }
}

/// A helper function that performes a vector write to completion, since
/// the `tokio` one is not guaranteed to write all of the data.
async fn write_all_vectored<'a, S: AsyncRead + AsyncWrite + Unpin>(
    s: &'a mut S,
    mut slices: &'a mut [IoSlice<'a>],
) -> io::Result<()> {
    let mut n: usize = slices.iter().map(|s| s.len()).sum();

    loop {
        let mut did_write = s.write_vectored(slices).await?;

        if did_write == n {
            // Done, yay
            break Ok(());
        }

        n -= did_write;

        // Not done, need to advance the slices
        while did_write >= slices[0].len() {
            // First skip entire slices
            did_write -= slices[0].len();
            slices = &mut slices[1..];
        }

        // Skip a partial buffer
        slices[0].advance(did_write);
    }
}

// Gets an IoSlice to each of the packets currently enqueued in `queue`.
fn queued_packet_slices(queue: &[QueuedPacket]) -> Vec<IoSlice<'_>> {
    if queue.is_empty() {
        return Vec::new();
    }

    let mut slices = Vec::with_capacity(queue.len() * 2);
    queue.iter().for_each(|packet| match packet {
        QueuedPacket::WithHeader(hdr, pack) => {
            slices.push(IoSlice::new(hdr));
            slices.push(IoSlice::new(pack));
        }
        QueuedPacket::Raw(r) => {
            slices.push(IoSlice::new(r));
        }
        QueuedPacket::Plain(p) => {
            slices.push(IoSlice::new(p));
        }
        QueuedPacket::LargePacket { headers, data } => {
            let mut remaining = data.len();
            let mut offset = 0;

            for header in headers {
                let chunk_size = remaining.min(MAX_PACKET_CHUNK_SIZE);
                slices.push(IoSlice::new(header));
                slices.push(IoSlice::new(&data[offset..offset + chunk_size]));

                offset += chunk_size;
                remaining -= chunk_size;
            }
        }
    });
    slices
}
impl<S: AsyncRead + AsyncWrite + Unpin> PacketConn<S> {
    pub fn new(s: S) -> Self {
        PacketConn {
            buffer: BytesMut::with_capacity(4096),
            seq: 0,
            queue: Vec::new(),
            preallocated: Vec::new(),
            stream: SwitchableStream::new(s),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> PacketConn<S> {
    pub fn set_seq(&mut self, seq: u8) {
        self.seq = seq;
    }

    pub fn next_seq(&mut self) -> u8 {
        let val = self.seq;
        self.seq = self.seq.wrapping_add(1);
        val
    }

    /// Flushes the writer. This function *must* be called before dropping the internal writer
    /// or writes may be lossed.
    pub async fn flush(&mut self) -> Result<(), tokio::io::Error> {
        self.write_queued_packets().await?;
        self.stream.flush().await
    }

    #[inline]
    fn make_header_bytes(len: usize, seq: u8) -> [u8; 4] {
        let mut hdr = (len as u32).to_le_bytes();
        hdr[3] = seq;
        hdr
    }

    #[inline]
    pub fn packet_header_bytes(&mut self, len: usize) -> [u8; 4] {
        Self::make_header_bytes(len, self.next_seq())
    }

    /// Push a new mysql packet to the outgoing packet list.
    pub fn enqueue_packet(&mut self, mut packet: Vec<u8>) {
        // Lazily shrink large buffers before processing them further, as after that they will go to
        // the buffer pool. This would occur if the buffer was reused, and the previous use was
        // greater than `MAX_POOL_ROW_CAPACITY`, and this use is less than `MAX_POOL_ROW_CAPACITY`.
        packet.shrink_to(MAX_POOL_ROW_CAPACITY);

        if packet.len() < MAX_PACKET_CHUNK_SIZE {
            let header = self.packet_header_bytes(packet.len());
            self.queue.push(QueuedPacket::WithHeader(header, packet));
        } else {
            self.enqueue_large_packet(packet);
        }
    }

    /// Push a new, very large mysql packet to the outgoing packet list. This function expects no
    /// packet header at the front of the buffer; in other words, just the packet payload data.
    pub(crate) fn enqueue_large_packet(&mut self, packet: Vec<u8>) {
        let mut headers = Vec::new();
        let mut remaining_len = packet.len();

        // build up the packet headers and keep with the data
        while remaining_len >= MAX_PACKET_CHUNK_SIZE {
            let header = self.packet_header_bytes(MAX_PACKET_CHUNK_SIZE);
            headers.push(header);
            remaining_len -= MAX_PACKET_CHUNK_SIZE;
        }

        // add the last header, even if the remaining length is 0 (as per mysql protocol)
        let header = self.packet_header_bytes(remaining_len);
        headers.push(header);

        self.queue.push(QueuedPacket::LargePacket {
            headers,
            data: packet,
        });
    }

    /// Enqueues raw, non-reference counted bytes to be written on the wire. It is assumed the `packet` is already a valid mysql packet(s).
    pub fn enqueue_plain(&mut self, packet: Vec<u8>) {
        self.queue.push(QueuedPacket::Plain(packet));
    }

    /// Enqueues raw, reference counted bytes to be written on the wire. It is assumed the `packet` is already a valid mysql packet(s).
    pub fn enqueue_raw(&mut self, packet: Arc<[u8]>) {
        self.queue.push(QueuedPacket::Raw(packet));
    }

    /// Send all the currently queued packets. Does not flush the writer.
    pub async fn write_queued_packets(&mut self) -> Result<(), tokio::io::Error> {
        let mut slices = queued_packet_slices(&self.queue);
        if !slices.is_empty() {
            write_all_vectored(&mut self.stream, &mut slices).await?;
            self.return_queued_to_pool();
        }

        Ok(())
    }

    /// Clear the queued packets and return them to the pool of preallocated packets
    fn return_queued_to_pool(&mut self) {
        self.queue.retain(|p| p.poolable());
        self.preallocated.append(&mut self.queue);
        self.preallocated.sort_by_key(|p| p.len());
        self.preallocated.truncate(MAX_POOL_BUFFERS);
    }

    pub fn get_buffer(&mut self) -> Vec<u8> {
        while let Some(p) = self.preallocated.pop() {
            match p {
                QueuedPacket::Raw(_) => {}
                QueuedPacket::WithHeader(_, mut vec)
                | QueuedPacket::Plain(mut vec)
                | QueuedPacket::LargePacket {
                    headers: _,
                    data: mut vec,
                } => {
                    vec.clear();
                    return vec;
                }
            }
        }
        Vec::new()
    }
}

pub struct Packet {
    // The actual data of the packet, without the header.
    pub data: BytesMut,
    // The sequence number of the packet.
    pub seq: u8,
    // The number of segments in the packet
    segments: u8,
}

impl Packet {
    pub fn new(data: BytesMut, seq: u8) -> Self {
        Packet {
            data,
            seq,
            segments: 1,
        }
    }

    fn append(&mut self, segment: BytesMut) {
        self.data.unsplit(segment);
        self.segments += 1;
    }
}

impl AsRef<[u8]> for Packet {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Deref for Packet {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

// Helper enum to properly handle incomplete reads
enum ParseResult {
    Complete { packet: Packet },
    // If a read is incomplete, and we need to read more bytes, we need to keep track of the already read bytes.
    Incomplete { packet: Option<Packet> },
}

macro_rules! parse_packet_header {
    ($buffer:expr) => {{
        let length = $buffer[0] as usize | ($buffer[1] as usize) << 8 | ($buffer[2] as usize) << 16;
        let seq = $buffer[3];
        (length, seq)
    }};
}

impl<S: AsyncRead + AsyncWrite + Unpin> PacketConn<S> {
    pub async fn next(&mut self) -> io::Result<Option<Packet>> {
        let mut in_progress = None;

        loop {
            match self.parse_packet(in_progress.take())? {
                ParseResult::Complete { packet } => return Ok(Some(packet)),
                ParseResult::Incomplete { packet } => {
                    in_progress = packet;
                    let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
                    if bytes_read == 0 {
                        return if self.buffer.is_empty() && in_progress.is_none() {
                            Ok(None)
                        } else {
                            Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!("{} unhandled bytes", self.buffer.len()),
                            ))
                        };
                    }
                }
            }
        }
    }

    fn parse_packet(&mut self, in_progress: Option<Packet>) -> io::Result<ParseResult> {
        // If we have an in-progress packet, continue with that. This is generally the case when
        // we are in the middle of reading a huge payload that spans multiple packets (_not_ the common case, at all).
        if let Some(packet) = in_progress {
            return self.handle_in_progress_packet(packet);
        }

        // Need at least 4 bytes to parse the header of the packet
        if self.buffer.len() < 4 {
            return Ok(ParseResult::Incomplete { packet: None });
        }

        let (length, seq) = parse_packet_header!(self.buffer);

        if self.buffer.len() < length + 4 {
            return Ok(ParseResult::Incomplete { packet: None });
        }

        // Remove header
        self.buffer.advance(4);

        // Handle large packets (>=16MB) as mysql treats those specially
        if length == MAX_PACKET_CHUNK_SIZE {
            let packet = Packet::new(self.buffer.split_to(MAX_PACKET_CHUNK_SIZE), seq);
            self.handle_in_progress_packet(packet)
        } else {
            // Regular packet - split off exactly what we need
            let data = self.buffer.split_to(length);
            Ok(ParseResult::Complete {
                packet: Packet::new(data, seq),
            })
        }
    }

    fn handle_in_progress_packet(&mut self, mut packet: Packet) -> io::Result<ParseResult> {
        loop {
            match self.parse_next_segment()? {
                Some(segment) => {
                    if segment.seq != packet.seq + packet.segments {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "expected seq {}, got {}",
                                packet.seq + packet.segments,
                                segment.seq
                            ),
                        ));
                    }

                    let segment_len = segment.len();
                    packet.append(segment.data);

                    if segment_len < MAX_PACKET_CHUNK_SIZE {
                        break;
                    }
                }
                None => {
                    return Ok(ParseResult::Incomplete {
                        packet: Some(packet),
                    })
                }
            }
        }
        Ok(ParseResult::Complete { packet })
    }

    fn parse_next_segment(&mut self) -> io::Result<Option<Packet>> {
        if self.buffer.len() < 4 {
            return Ok(None);
        }

        let (length, seq) = parse_packet_header!(self.buffer);

        if self.buffer.len() < length + 4 {
            return Ok(None);
        }

        // Remove header
        self.buffer.advance(4);

        // Split off the segment
        Ok(Some(Packet::new(self.buffer.split_to(length), seq)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple_packet() {
        let (u_out, u_in) = tokio::net::UnixStream::pair().unwrap();

        tokio::spawn(async move {
            let mut writer = PacketConn::new(u_out);
            writer.enqueue_packet(vec![0x10]);
            writer.flush().await.unwrap();
        });

        let mut reader = PacketConn::new(u_in);
        let packet = reader.next().await.unwrap().unwrap();

        assert_eq!(packet.seq, 0);
        assert_eq!(&*packet, &[0x10]);
        assert_eq!(packet.segments, 1);

        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_exact_size_packet() {
        let (u_out, u_in) = tokio::net::UnixStream::pair().unwrap();
        let data = vec![0; MAX_PACKET_CHUNK_SIZE];
        let data_clone = data.clone();

        tokio::spawn(async move {
            let mut writer = PacketConn::new(u_out);
            writer.enqueue_packet(data_clone);
            writer.flush().await.unwrap();
        });

        let mut reader = PacketConn::new(u_in);
        let packet = reader.next().await.unwrap().unwrap();

        assert_eq!(packet.seq, 0);
        assert_eq!(packet.len(), MAX_PACKET_CHUNK_SIZE);
        assert_eq!(&*packet, &data);
        assert_eq!(packet.segments, 2); // One full segment + empty final segment

        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_large_packet() {
        let (u_out, u_in) = tokio::net::UnixStream::pair().unwrap();
        let mut data = vec![0; MAX_PACKET_CHUNK_SIZE];
        data.extend_from_slice(&[0x10]);

        tokio::spawn(async move {
            let mut writer = PacketConn::new(u_out);
            writer.enqueue_packet(data);
            writer.flush().await.unwrap();
        });

        let mut reader = PacketConn::new(u_in);
        let packet = reader.next().await.unwrap().unwrap();

        assert_eq!(packet.seq, 0);
        assert_eq!(packet.len(), MAX_PACKET_CHUNK_SIZE + 1);
        assert_eq!(
            &packet[..MAX_PACKET_CHUNK_SIZE],
            &[0; MAX_PACKET_CHUNK_SIZE]
        );
        assert_eq!(&packet[MAX_PACKET_CHUNK_SIZE..], &[0x10]);
        //assert_eq!(packet.segments, 2);

        assert!(reader.next().await.unwrap().is_none());
    }

    async fn test_large_packet_write_helper<F, Fut>(write_strategy: F)
    where
        F: FnOnce(PacketConn<tokio::net::UnixStream>, Vec<Vec<u8>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = io::Result<()>> + Send,
    {
        let (u_out, u_in) = tokio::net::UnixStream::pair().unwrap();

        // send multiple packets, of varying sizes.
        let packets = vec![
            vec![0u8; 245],                         // 1 packet
            vec![1u8; MAX_PACKET_CHUNK_SIZE * 2],   // 3 packets (2 full + 1 empty)
            vec![2u8; MAX_PACKET_CHUNK_SIZE + 100], // 2 packets
            vec![3u8; 100],                         // 1 packet
            vec![4u8; MAX_PACKET_CHUNK_SIZE - 1],   // 1 packet
            vec![5u8; MAX_PACKET_CHUNK_SIZE],       // 2 packets (1 full + 1 empty)
        ];

        let p = packets.clone();
        tokio::spawn(async move {
            let writer = PacketConn::new(u_out);
            write_strategy(writer, p).await.unwrap();
        });

        let mut reader = PacketConn::new(u_in);

        // Verify all packets were received correctly
        for expected in &packets {
            let packet = reader.next().await.unwrap().unwrap();
            assert_eq!(&*packet, expected.as_slice());

            // Verify segment count
            let needs_empty_segment = expected.len() % MAX_PACKET_CHUNK_SIZE == 0;
            let expected_segments =
                expected.len().div_ceil(MAX_PACKET_CHUNK_SIZE) + usize::from(needs_empty_segment);
            assert_eq!(packet.segments as usize, expected_segments);
        }

        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_large_packet_write_queued() {
        test_large_packet_write_helper(|mut writer, packets| async move {
            for packet in packets {
                writer.enqueue_packet(packet);
            }
            writer.write_queued_packets().await?;
            writer.flush().await
        })
        .await;
    }
}
