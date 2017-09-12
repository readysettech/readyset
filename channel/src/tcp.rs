use std;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::SocketAddr;

use bincode::{self, Infinite};
use bufstream::BufStream;
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use mio::{self, Evented, Poll, PollOpt, Ready, Token};
use serde::{Deserialize, Serialize};

use super::{DeserializeReceiver, NonBlockingWriter, ReceiveError};

#[derive(Debug)]
pub enum SendError {
    BincodeError(bincode::Error),
    IoError(io::Error),
    Poisoned,
}

impl From<bincode::Error> for SendError {
    fn from(e: bincode::Error) -> Self {
        SendError::BincodeError(e)
    }
}

impl From<io::Error> for SendError {
    fn from(e: io::Error) -> Self {
        SendError::IoError(e)
    }
}

macro_rules! poisoning_try {
    ( $self_:ident, $e:expr ) => {
        match $e {
            Ok(v) => v,
            Err(r) => {
                $self_.poisoned = true;
                return Err(r.into())
            }
        }
    }
}

pub struct TcpSender<T> {
    stream: BufStream<std::net::TcpStream>,
    window: Option<u32>,
    unacked: u32,
    poisoned: bool,

    phantom: PhantomData<T>,
}

impl<T: Serialize> TcpSender<T> {
    pub fn new(mut stream: std::net::TcpStream, window: Option<u32>) -> Result<Self, io::Error> {
        if let Some(window) = window {
            assert!(window > 0);
        }

        stream.write_u32::<NetworkEndian>(window.unwrap_or(0))?;

        Ok(Self {
            stream: BufStream::new(stream),
            window,
            unacked: 0,
            poisoned: false,
            phantom: PhantomData,
        })
    }

    pub fn connect(addr: &SocketAddr, window: Option<u32>) -> Result<Self, io::Error> {
        Self::new(std::net::TcpStream::connect(addr)?, window)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().local_addr()
    }

    /// Send a message on this channel. Ownership isn't actually required, but is taken anyway to
    /// conform to the same api as mpsc::Sender.
    pub fn send(&mut self, t: T) -> Result<(), SendError> {
        self.send_ref(&t)
    }

    pub fn send_ref(&mut self, t: &T) -> Result<(), SendError> {
        if self.poisoned {
            return Err(SendError::Poisoned);
        }

        if let Some(window) = self.window {
            if self.unacked == window {
                let mut buf = [0u8];
                poisoning_try!(self, self.stream.read_exact(&mut buf));
                self.unacked = 0;
            }
            self.unacked += 1;
        }

        let size: u32 = bincode::serialized_size(t) as u32;
        poisoning_try!(self, self.stream.write_u32::<NetworkEndian>(size));
        poisoning_try!(self, bincode::serialize_into(&mut self.stream, t, Infinite));
        poisoning_try!(self, self.stream.flush());
        Ok(())
    }
}

#[derive(Debug)]
pub enum TryRecvError {
    Empty,
    Disconnected,
    DeserializationError,
}

#[derive(Debug)]
pub enum RecvError {
    Disconnected,
    DeserializationError,
}

#[derive(Default)]
struct Buffer {
    data: Vec<u8>,
    size: usize,
}

impl Buffer {
    pub fn fill_from<T: Read>(
        &mut self,
        stream: &mut T,
        target_size: usize,
    ) -> Result<(), io::Error> {
        if self.data.len() < target_size {
            self.data.resize(target_size, 0u8);
        }

        while self.size < target_size {
            let n = stream.read(&mut self.data[self.size..target_size])?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
            self.size += n;
        }
        Ok(())
    }
}

pub struct TcpReceiver<T> {
    pub(crate) stream: NonBlockingWriter<mio::net::TcpStream>,
    unacked: u32,
    poisoned: bool,

    // A value of zero for window makes the channel unbounded. None means that the window size is
    // not yet known.
    window: Option<u32>,
    // Holds the bytes of the window size until it is known.
    window_buf: Buffer,

    deserialize_receiver: DeserializeReceiver<T>,

    phantom: PhantomData<T>,
}

impl<T> TcpReceiver<T>
where
    for<'de> T: Deserialize<'de>,
{
    pub fn new(stream: mio::net::TcpStream) -> Self {
        Self {
            stream: NonBlockingWriter::new(stream),
            unacked: 0,
            poisoned: false,
            window: None,
            window_buf: Buffer {
                data: vec![0u8; 4],
                size: 0,
            },
            deserialize_receiver: DeserializeReceiver::new(),
            phantom: PhantomData,
        }
    }

    pub fn listen(addr: &SocketAddr) -> Result<Self, io::Error> {
        let listener = mio::net::TcpListener::bind(addr)?;
        Ok(Self::new(listener.accept()?.0))
    }

    pub fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        self.stream.get_ref().local_addr()
    }

    fn send_ack(&mut self) {
        match self.stream.write(&[0u8]) {
            Ok(n) => assert_eq!(n, 1),
            Err(ref e) if e.kind() == io::ErrorKind::BrokenPipe => {}
            Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => {}
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => unreachable!(),
            Err(_) => self.poisoned = true,
        }
        self.unacked = 0;
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if self.poisoned {
            return Err(TryRecvError::Disconnected);
        }

        if self.window.is_none() {
            match self.window_buf.fill_from(&mut self.stream, 4) {
                Ok(()) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Err(TryRecvError::Empty)
                }
                _ => {
                    self.poisoned = true;
                    return Err(TryRecvError::Disconnected);
                }
            }
            self.window = Some(NetworkEndian::read_u32(&self.window_buf.data[0..4]));
        }

        // Make sure that any previously issued ACKs are sent out on the wire.
        match self.stream.flush() {
            Ok(()) => {}
            Err(ref e) if e.kind() == io::ErrorKind::BrokenPipe => {}
            Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => {}
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Err(TryRecvError::Empty),
            Err(_) => {
                self.poisoned = true;
                return Err(TryRecvError::Disconnected);
            }
        }

        match self.deserialize_receiver.try_recv(&mut self.stream) {
            Ok(msg) => {
                self.unacked = self.unacked.saturating_add(1);
                if self.unacked == *self.window.as_ref().unwrap() {
                    self.send_ack();
                }
                Ok(msg)
            }
            Err(ReceiveError::WouldBlock) => Err(TryRecvError::Empty),
            Err(ReceiveError::IoError(_)) => {
                self.poisoned = true;
                Err(TryRecvError::Disconnected)
            }
            Err(ReceiveError::DeserializationError(_)) => {
                self.poisoned = true;
                Err(TryRecvError::DeserializationError)
            }
        }
    }

    pub fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            return match self.try_recv() {
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
                Err(TryRecvError::DeserializationError) => Err(RecvError::DeserializationError),
                Ok(t) => Ok(t),
            };
        }
    }
}

impl<T> Evented for TcpReceiver<T> {
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.stream.get_ref().register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.stream
            .get_ref()
            .reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.stream.get_ref().deregister(poll)
    }
}

fn connect() -> (std::net::TcpStream, mio::net::TcpStream) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = std::net::TcpListener::bind(&addr).unwrap();
    let rx = mio::net::TcpStream::connect(&listener.local_addr().unwrap()).unwrap();
    let tx = listener.accept().unwrap().0;

    (tx, rx)
}

pub fn channel<T: Serialize>() -> (TcpSender<T>, TcpReceiver<T>)
where
    for<'de> T: Deserialize<'de>,
{
    let (tx, rx) = connect();
    let tx = TcpSender::new(tx, None).unwrap();
    let rx = TcpReceiver::new(rx);
    (tx, rx)
}

pub fn sync_channel<T: Serialize>(size: u32) -> (TcpSender<T>, TcpReceiver<T>)
where
    for<'de> T: Deserialize<'de>,
{
    let (tx, rx) = connect();
    let tx = TcpSender::new(tx, Some(size)).unwrap();
    let rx = TcpReceiver::new(rx);
    (tx, rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use mio::Events;

    #[test]
    fn unbounded() {
        let (mut sender, mut receiver) = channel::<u32>();

        sender.send(12).unwrap();
        assert_eq!(receiver.recv().unwrap(), 12);

        sender.send(65).unwrap();
        sender.send(13).unwrap();
        assert_eq!(receiver.recv().unwrap(), 65);
        assert_eq!(receiver.recv().unwrap(), 13);
    }

    #[test]
    fn bounded() {
        let (mut sender, mut receiver) = sync_channel::<u32>(2);

        sender.send(12).unwrap();
        sender.send(65).unwrap();
        assert_eq!(receiver.recv().unwrap(), 12);
        assert_eq!(receiver.recv().unwrap(), 65);

        sender.send(13).unwrap();
        assert_eq!(receiver.recv().unwrap(), 13);
    }

    #[test]
    fn bounded_multithread() {
        let (mut sender, mut receiver) = sync_channel::<u32>(2);

        let t1 = thread::spawn(move || {
            sender.send(12).unwrap();
            sender.send(65).unwrap();
            sender.send(13).unwrap();
        });

        let t2 = thread::spawn(move || {
            assert_eq!(receiver.recv().unwrap(), 12);
            assert_eq!(receiver.recv().unwrap(), 65);
            assert_eq!(receiver.recv().unwrap(), 13);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn poll() {
        let (mut sender, mut receiver) = sync_channel::<u32>(2);

        let t1 = thread::spawn(move || {
            sender.send(12).unwrap();
            sender.send(65).unwrap();
            sender.send(13).unwrap();
        });

        let t2 = thread::spawn(move || {
            let poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();
            poll.poll(&mut events, None).unwrap();
            let mut events = events.into_iter();
            assert_eq!(events.next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 12);
            assert_eq!(receiver.recv().unwrap(), 65);
            assert_eq!(receiver.recv().unwrap(), 13);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn ping_pong() {
        let (mut sender, mut receiver) = sync_channel::<u32>(1);
        let (mut sender2, mut receiver2) = sync_channel::<u32>(1);

        let t1 = thread::spawn(move || {
            let poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 15);

            sender2.send(12).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 54);

            sender2.send(65).unwrap();
        });

        let t2 = thread::spawn(move || {
            let poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver2, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();

            sender.send(15).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver2.recv().unwrap(), 12);

            sender.send(54).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver2.recv().unwrap(), 65);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
