use std::io::{self, IoSlice};
use std::ops::Deref;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::resultset::{MAX_POOL_ROWS, MAX_POOL_ROW_CAPACITY};

/// The maximum length of a mysql packet, in bytes.
///
/// Note: this value is a similar function to mysql's `max_allowed_packet`.
/// In mysql 5.7, this value defaults to 4 MiB. In 8.0+, it defaults to 64 MiB.
/// mysql allows this to be set at the global and session levels, but YOLO ...
const U24_MAX: usize = 16_777_215;

pub struct PacketWriter<W> {
    pub seq: u8,
    w: W,
    queue: Vec<QueuedPacket>,

    /// Reusable packets
    preallocated: Vec<QueuedPacket>,
}

/// Flags for the `PacketWriter::get_buffer` method, to indicate if the to-be-allocated buffer
/// should include space for the packet header.
pub enum PacketHeaderFlag {
    /// Include the space for the packet header in the to-be-allocated buffer.
    /// Header will be written to the buffer when submitted to the writer.
    IncludeHeader,
    /// Skip the header in the packet
    #[allow(unused)] // will be enabled in a followup patch
    SkipHeader,
}

/// Type for packets being enqueued in the packet writer.
enum QueuedPacket {
    /// Raw, reference-counted queued packets are written as-is; these packets include header chunks.
    Raw(Arc<[u8]>),
    /// Raw, non-reference-counted queued packets are written as-is; these packets include header chunks.
    Plain(Vec<u8>),
    /// Packets which do not include their headers are written as two IoSlices:
    /// one for the header and one for the body.
    WithHeader([u8; 4], Vec<u8>),
}

/// A helper function that performes a vector write to completion, since
/// the `tokio` one is not guaranteed to write all of the data.
async fn write_all_vectored<'a, W: AsyncWrite + Unpin>(
    w: &'a mut W,
    mut slices: &'a mut [IoSlice<'a>],
) -> io::Result<()> {
    let mut n: usize = slices.iter().map(|s| s.len()).sum();

    loop {
        let mut did_write = w.write_vectored(slices).await?;

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
        QueuedPacket::Plain(buf) => {
            slices.push(IoSlice::new(buf));
        }
    });

    slices
}

macro_rules! write_packet_header {
    ($packet:expr, $len:expr, $seq:expr) => {
        let len_bytes = ($len as u32).to_le_bytes();
        $packet[0] = len_bytes[0];
        $packet[1] = len_bytes[1];
        $packet[2] = len_bytes[2];
        $packet[3] = $seq;
    };
}

impl<W: AsyncWrite + Unpin> PacketWriter<W> {
    pub fn new(w: W) -> Self {
        PacketWriter {
            seq: 0,
            w,
            queue: Vec::new(),
            preallocated: Vec::new(),
        }
    }

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
        self.w.flush().await
    }

    /// Push a new mysql packet to the outgoing packet list.
    ///
    /// This function assumes that there is space for the packet header at the beginning of the packet;
    /// those bytes will be overwritten for the packet header.
    ///
    /// If you are going to call this function, it is highly recommended to use `get_buffer` with
    /// `PacketHeaderFlag::IncludeHeader` to get a buffer that includes space for the packet header.
    pub fn enqueue_packet(&mut self, mut packet: Vec<u8>) {
        // Lazily shrink large buffers before processing them further, as after that they will go to
        // the buffer pool. This would occur if the buffer was resued, and the previous use was
        // greater than `MAX_POOL_ROW_CAPACITY`, and this use is less than `MAX_POOL_ROW_CAPACITY`.
        packet.shrink_to(MAX_POOL_ROW_CAPACITY);

        if packet.len() < U24_MAX {
            let packet_len = packet.len() - 4;
            write_packet_header!(packet, packet_len, self.next_seq());
            self.queue.push(QueuedPacket::Plain(packet));
        } else {
            self.enqueue_large_packet(packet);
        }
    }

    /// Push a new, very large mysql packet to the outgoing packet list.
    ///
    /// This function assumes that there is space for the packet header at the beginning of the packet;
    /// meaning, the caller allocated the buffer from `get_buffer` (with `PacketHeaderFlag::IncludeHeader`).
    pub(crate) fn enqueue_large_packet(&mut self, mut packet: Vec<u8>) {
        macro_rules! write_packet_with_header {
            ($packet:expr, $len:expr) => {
                let mut hdr = (U24_MAX as u32).to_le_bytes();
                hdr[3] = self.next_seq();
                self.queue.push(QueuedPacket::WithHeader(hdr, $packet));
            };
        }

        let mut remaining_len = packet.len();
        let _ = packet.drain(0..4);
        remaining_len -= 4;

        while remaining_len >= U24_MAX {
            let rest = packet.split_off(U24_MAX);
            write_packet_with_header!(packet, U24_MAX);
            remaining_len -= U24_MAX;
            packet = rest;
        }

        write_packet_with_header!(packet, remaining_len);
    }

    /// Enqueues raw bytes to be written on the wire.
    pub async fn enqueue_raw(&mut self, packet: Arc<[u8]>) -> Result<(), tokio::io::Error> {
        self.queue.push(QueuedPacket::Raw(packet));
        Ok(())
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// Send all the currently queued packets. Does not flush the writer.
    pub async fn write_queued_packets(&mut self) -> Result<(), tokio::io::Error> {
        let mut slices = queued_packet_slices(&self.queue);
        if !slices.is_empty() {
            write_all_vectored(&mut self.w, &mut slices).await?;
            self.return_queued_to_pool();
        }

        Ok(())
    }

    /// Clear the queued packets and return them to the pool of preallocated packets
    fn return_queued_to_pool(&mut self) {
        // Prefer to merge the shorter vector into the longer vector, thus minimizing the amount of
        // copying necessary. i.e. if `queue` already contains all the allocated vectors, no action
        // is needed.
        if self.queue.len() > self.preallocated.len() {
            std::mem::swap(&mut self.queue, &mut self.preallocated);
        }
        // Limit the number of pre allocated buffers to `MAX_POOL_ROWS`
        self.preallocated.truncate(MAX_POOL_ROWS);
        self.queue.truncate(MAX_POOL_ROWS - self.preallocated.len());
        self.preallocated.append(&mut self.queue);
    }

    /// Get a buffer of the specified size. If the `header_flag` is `IncludeHeader`,
    /// the buffer will include space for the packet header. The caller is _not_
    /// responsible for writing the header, it will be written by the writer when
    /// the buffer is submitted to this writer.
    pub fn get_buffer(&mut self, size: usize, header_flag: PacketHeaderFlag) -> Vec<u8> {
        let target_size = size
            + match header_flag {
                PacketHeaderFlag::IncludeHeader => 4,
                PacketHeaderFlag::SkipHeader => 0,
            };

        let mut vec = loop {
            match self.preallocated.pop() {
                Some(QueuedPacket::Raw(_)) => continue,
                Some(QueuedPacket::Plain(mut vec)) | Some(QueuedPacket::WithHeader(_, mut vec)) => {
                    vec.clear();
                    vec.reserve(target_size);
                    break vec;
                }
                None => break Vec::with_capacity(target_size),
            }
        };

        if matches!(header_flag, PacketHeaderFlag::IncludeHeader) {
            vec.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        }
        vec
    }
}

pub struct PacketReader<R> {
    // A buffer to hold incoming socket bytes, while building up for a complete packet.
    buffer: BytesMut,
    r: R,
}

impl<R> PacketReader<R> {
    pub fn new(r: R) -> Self {
        PacketReader {
            buffer: BytesMut::with_capacity(4096),
            r,
        }
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

impl<R: AsyncRead + Unpin> PacketReader<R> {
    pub async fn next(&mut self) -> io::Result<Option<Packet>> {
        let mut in_progress = None;

        loop {
            match self.parse_packet(in_progress.take())? {
                ParseResult::Complete { packet } => return Ok(Some(packet)),
                ParseResult::Incomplete { packet } => {
                    in_progress = packet;
                    let bytes_read = self.r.read_buf(&mut self.buffer).await?;
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
        if length == U24_MAX {
            let packet = Packet::new(self.buffer.split_to(U24_MAX), seq);
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

                    if segment_len < U24_MAX {
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
        let mut writer = PacketWriter::new(u_out);

        let packets = vec![
            writer.get_buffer(245, PacketHeaderFlag::IncludeHeader),
            writer.get_buffer(U24_MAX * 2, PacketHeaderFlag::IncludeHeader),
            writer.get_buffer(U24_MAX + 100, PacketHeaderFlag::IncludeHeader),
            writer.get_buffer(100, PacketHeaderFlag::IncludeHeader),
            writer.get_buffer(U24_MAX - 1, PacketHeaderFlag::IncludeHeader),
            writer.get_buffer(U24_MAX, PacketHeaderFlag::IncludeHeader),
        ];

        let p = packets.clone();
        tokio::spawn(async move {
            for packet in &p {
                writer.enqueue_packet(packet.clone());
            }

            writer.flush().await.unwrap();
        });

        let mut reader = PacketReader::new(u_in);
        let packet = reader.next().await.unwrap().unwrap();

        assert_eq!(packet.seq, 0);
        assert_eq!(&*packet, &[0x10]);
        assert_eq!(packet.segments, 1);

        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_exact_size_packet() {
        let (u_out, u_in) = tokio::net::UnixStream::pair().unwrap();
        let data = vec![0; U24_MAX];
        let data_clone = data.clone();

        tokio::spawn(async move {
            let mut writer = PacketWriter::new(u_out);
            writer.enqueue_packet(data_clone);
            writer.flush().await.unwrap();
        });

        let mut reader = PacketReader::new(u_in);
        let packet = reader.next().await.unwrap().unwrap();

        assert_eq!(packet.seq, 0);
        assert_eq!(packet.len(), U24_MAX);
        assert_eq!(&*packet, &data);
        assert_eq!(packet.segments, 2); // One full segment + empty final segment

        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_large_packet() {
        let (u_out, u_in) = tokio::net::UnixStream::pair().unwrap();
        let mut data = vec![0; U24_MAX];
        data.extend_from_slice(&[0x10]);

        tokio::spawn(async move {
            let mut writer = PacketWriter::new(u_out);
            writer.enqueue_packet(data);
            writer.flush().await.unwrap();
        });

        let mut reader = PacketReader::new(u_in);
        let packet = reader.next().await.unwrap().unwrap();

        assert_eq!(packet.seq, 0);
        assert_eq!(packet.len(), U24_MAX + 1);
        assert_eq!(&packet[..U24_MAX], &[0; U24_MAX]);
        assert_eq!(&packet[U24_MAX..], &[0x10]);
        //assert_eq!(packet.segments, 2);

        assert!(reader.next().await.unwrap().is_none());
    }

    async fn test_large_packet_write_helper<F, Fut>(write_strategy: F)
    where
        F: FnOnce(PacketWriter<tokio::net::UnixStream>, Vec<Vec<u8>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = io::Result<()>> + Send,
    {
        let (u_out, u_in) = tokio::net::UnixStream::pair().unwrap();

        // send multiple packets, of varying sizes.
        let packets = vec![
            vec![0u8; 245],           // 1 packet
            vec![1u8; U24_MAX * 2],   // 3 packets (2 full + 1 empty)
            vec![2u8; U24_MAX + 100], // 2 packets
            vec![3u8; 100],           // 1 packet
            vec![4u8; U24_MAX - 1],   // 1 packet
            vec![5u8; U24_MAX],       // 2 packets (1 full + 1 empty)
        ];

        let p = packets.clone();
        tokio::spawn(async move {
            let writer = PacketWriter::new(u_out);
            write_strategy(writer, p).await.unwrap();
        });

        let mut reader = PacketReader::new(u_in);

        // Verify all packets were received correctly
        for expected in &packets {
            let packet = reader.next().await.unwrap().unwrap();
            assert_eq!(&*packet, expected.as_slice());

            // Verify segment count
            let needs_empty_segment = expected.len() % U24_MAX == 0;
            let expected_segments =
                expected.len().div_ceil(U24_MAX) + usize::from(needs_empty_segment);
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
