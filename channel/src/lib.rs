#![feature(bufreader_buffer)]
#![feature(ip_constructors)]
#![feature(custom_attribute)]
#![feature(try_from)]
#![deny(unused_extern_crates)]

extern crate bincode;
extern crate bufstream;
extern crate byteorder;
extern crate mio;
extern crate net2;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate throttled_reader;

use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc::{self, SendError};
use std::sync::Mutex;

use byteorder::{ByteOrder, NetworkEndian};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod poll;
pub mod rpc;
pub mod tcp;

pub use tcp::{channel, DualTcpReceiver, TcpReceiver, TcpSender};

pub const CONNECTION_FROM_MUTATOR: u8 = 1;
pub const CONNECTION_FROM_DOMAIN: u8 = 0;

pub struct DomainConnectionBuilder {
    sport: Option<u16>,
    addr: SocketAddr,
    is_for_mutator: bool,
}

impl DomainConnectionBuilder {
    pub fn for_mutator(addr: SocketAddr) -> Self {
        DomainConnectionBuilder {
            sport: None,
            addr,
            is_for_mutator: true,
        }
    }

    pub fn for_domain(addr: SocketAddr) -> Self {
        DomainConnectionBuilder {
            sport: None,
            addr,
            is_for_mutator: false,
        }
    }

    pub fn maybe_on_port(mut self, sport: Option<u16>) -> Self {
        self.sport = sport;
        self
    }

    pub fn on_port(mut self, sport: u16) -> Self {
        self.sport = Some(sport);
        self
    }

    pub fn build<T: serde::Serialize>(self) -> io::Result<TcpSender<T>> {
        let mut s = TcpSender::connect_from(self.sport, &self.addr)?;
        {
            let s = s.get_mut();
            s.write_all(&[if self.is_for_mutator {
                CONNECTION_FROM_MUTATOR
            } else {
                CONNECTION_FROM_DOMAIN
            }])?;
            s.flush()?;
        }
        Ok(s)
    }
}

#[derive(Debug)]
pub enum ChannelSender<T> {
    Local(mpsc::Sender<T>),
    LocalSync(mpsc::SyncSender<T>),
}

impl<T> Clone for ChannelSender<T> {
    fn clone(&self) -> Self {
        // derive(Clone) uses incorrect bound, so we implement it ourselves. See issue #26925.
        match *self {
            ChannelSender::Local(ref s) => ChannelSender::Local(s.clone()),
            ChannelSender::LocalSync(ref s) => ChannelSender::LocalSync(s.clone()),
        }
    }
}

impl<T> Serialize for ChannelSender<T> {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        unreachable!()
    }
}

impl<'de, T> Deserialize<'de> for ChannelSender<T> {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        unreachable!()
    }
}

impl<T> ChannelSender<T> {
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        match *self {
            ChannelSender::Local(ref s) => s.send(t),
            ChannelSender::LocalSync(ref s) => s.send(t),
        }
    }

    pub fn from_local(local: mpsc::Sender<T>) -> Self {
        ChannelSender::Local(local)
    }
}

mod panic_serialize {
    use serde::{Deserializer, Serializer};
    pub fn serialize<S, T>(_t: &T, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        unreachable!()
    }
    pub fn deserialize<'de, D, T>(_deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
    {
        unreachable!()
    }
}

/// A wrapper around TcpSender that appears to be Serializable, but panics if it is ever serialized.
#[derive(Serialize, Deserialize)]
pub struct STcpSender<T>(#[serde(with = "panic_serialize")] pub TcpSender<T>);

impl<T> Deref for STcpSender<T> {
    type Target = TcpSender<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> DerefMut for STcpSender<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub type TraceSender<T> = ChannelSender<T>;
pub type TransactionReplySender<T> = ChannelSender<T>;
pub type StreamSender<T> = ChannelSender<T>;

struct ChannelCoordinatorInner<K: Eq + Hash + Clone> {
    /// Map from key to tuple of address and whether the endpoint is local.
    addrs: HashMap<K, (SocketAddr, bool)>,
}

pub struct ChannelCoordinator<K: Eq + Hash + Clone> {
    inner: Mutex<ChannelCoordinatorInner<K>>,
}

impl<K: Eq + Hash + Clone> ChannelCoordinator<K> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ChannelCoordinatorInner {
                addrs: HashMap::new(),
            }),
        }
    }

    pub fn insert_addr(&self, key: K, addr: SocketAddr, local: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.addrs.insert(key, (addr, local));
    }

    pub fn get_addr(&self, key: &K) -> Option<SocketAddr> {
        self.inner.lock().unwrap().addrs.get(key).map(|a| a.0)
    }

    pub fn is_local(&self, key: &K) -> Option<bool> {
        self.inner.lock().unwrap().addrs.get(key).map(|a| a.1)
    }

    pub fn get_dest(&self, key: &K) -> Option<(SocketAddr, bool)> {
        self.inner.lock().unwrap().addrs.get(key).cloned()
    }
}

/// A wrapper around a writer that handles `Error::WouldBlock` when attempting to write.
///
/// Instead of return that error, it places the bytes into a buffer so that subsequent calls to
/// `write()` can retry writing them.
pub struct NonBlockingWriter<T> {
    writer: T,
    buffer: Vec<u8>,
    cursor: usize,
}

impl<T: Write> NonBlockingWriter<T> {
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            buffer: Vec::new(),
            cursor: 0,
        }
    }

    pub fn needs_flush_to_inner(&self) -> bool {
        self.buffer.len() != self.cursor
    }

    pub fn flush_to_inner(&mut self) -> io::Result<()> {
        if self.buffer.len() > 0 {
            while self.cursor < self.buffer.len() {
                match self.writer.write(&self.buffer[self.cursor..])? {
                    0 => return Err(io::Error::from(io::ErrorKind::BrokenPipe)),
                    n => self.cursor += n,
                }
            }
            self.buffer.clear();
            self.cursor = 0;
        }
        Ok(())
    }

    pub fn get_ref(&self) -> &T {
        &self.writer
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.writer
    }
}

impl<T: Write> Write for NonBlockingWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        match self.flush_to_inner() {
            Ok(_) => Ok(buf.len()),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(buf.len()),
            Err(e) => {
                let old_len = self.buffer.len() - buf.len();
                self.buffer.truncate(old_len);
                Err(e)
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_to_inner()?;
        self.writer.flush()
    }
}
impl<T: Read> Read for NonBlockingWriter<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.writer.read(buf)
    }
}

#[derive(Debug)]
pub enum ReceiveError {
    WouldBlock,
    IoError(io::Error),
    DeserializationError(bincode::Error),
}

impl From<io::Error> for ReceiveError {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::WouldBlock {
            ReceiveError::WouldBlock
        } else {
            ReceiveError::IoError(error)
        }
    }
}
impl From<bincode::Error> for ReceiveError {
    fn from(error: bincode::Error) -> Self {
        ReceiveError::DeserializationError(error)
    }
}

#[derive(Default)]
pub struct DeserializeReceiver<T> {
    buffer: Vec<u8>,
    size: usize,
    phantom: PhantomData<T>,
}

impl<T> DeserializeReceiver<T>
where
    for<'a> T: Deserialize<'a>,
{
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            size: 0,
            phantom: PhantomData,
        }
    }

    fn fill_from<R: Read>(
        &mut self,
        stream: &mut R,
        target_size: usize,
    ) -> Result<(), ReceiveError> {
        if self.buffer.len() < target_size {
            self.buffer.resize(target_size, 0u8);
        }

        while self.size < target_size {
            let n = stream.read(&mut self.buffer[self.size..target_size])?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
            }
            self.size += n;
        }
        Ok(())
    }

    pub fn try_recv<R: Read>(&mut self, reader: &mut R) -> Result<T, ReceiveError> {
        if self.size < 4 {
            self.fill_from(reader, 5)?;
        }

        let message_size: u32 = NetworkEndian::read_u32(&self.buffer[0..4]);
        let target_buffer_size = message_size as usize + 4;
        self.fill_from(reader, target_buffer_size)?;

        let message = bincode::deserialize(&self.buffer[4..target_buffer_size])?;
        self.size = 0;
        Ok(message)
    }
}
