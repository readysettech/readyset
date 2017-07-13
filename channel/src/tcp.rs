
use std;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;

use bincode::{self, Infinite};
use bufstream::BufStream;
use byteorder::{ByteOrder, NetworkEndian, ReadBytesExt, WriteBytesExt};
use mio::{self, Evented, Poll, PollOpt, Ready, Token};
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum Error {
    BincodeError(bincode::Error),
    IoError(io::Error),
    Poisoned,
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::BincodeError(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IoError(e)
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
    window: Option<u64>,
    unacked: u64,
    poisoned: bool,

    phantom: PhantomData<T>,
}
impl<T: Serialize> TcpSender<T> {
    pub fn new(mut stream: std::net::TcpStream, window: Option<u64>) -> Result<Self, io::Error> {
        if let Some(window) = window {
            assert!(window > 0);
        }

        stream.write_u64::<NetworkEndian>(window.unwrap_or(0))?;

        Ok(Self {
            stream: BufStream::new(stream),
            window,
            unacked: 0,
            poisoned: false,
            phantom: PhantomData,
        })
    }

    pub fn connect(addr: &SocketAddr, window: Option<u64>) -> Result<Self, io::Error> {
        Self::new(std::net::TcpStream::connect(addr)?, window)
    }

    /// Send a message on this channel. Ownership isn't actually required, but is taken anyway to
    /// conform to the same api as mpsc::Sender.
    pub fn send(&mut self, t: T) -> Result<(), Error> {
        self.send_ref(&t)
    }

    pub fn send_ref(&mut self, t: &T) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }

        if Some(self.unacked) == self.window {
            let mut buf = [0u8];
            poisoning_try!(self, self.stream.read_exact(&mut buf));
            self.unacked = 0;
        }

        self.unacked += 1;

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

pub struct TcpReceiver<T> {
    stream: mio::net::TcpStream,
    window: Option<u64>,
    unacked: u64,
    poisoned: bool,

    // Holds data from the stream that is not yet been handled.
    buffer: Vec<u8>,
    // Amount of data in `buffer` that is valid.
    buffer_size: usize,

    phantom: PhantomData<T>,
}
impl<T> TcpReceiver<T>
where
    for<'de> T: Deserialize<'de>,
{
    pub fn new(mut stream: std::net::TcpStream) -> Result<Self, io::Error> {
        let window = stream.read_u64::<NetworkEndian>()?;
        let window = if window == 0 { None } else { Some(window) };

        Ok(Self {
            stream: mio::net::TcpStream::from_stream(stream)?,
            window,
            unacked: 0,
            buffer: vec![0; 1024],
            buffer_size: 0,
            poisoned: false,
            phantom: PhantomData,
        })
    }

    pub fn listen(addr: &SocketAddr) -> Result<Self, io::Error> {
        let listener = std::net::TcpListener::bind(addr)?;
        Self::new(listener.accept()?.0)
    }

    fn send_ack(&mut self) -> Result<(), io::Error> {
        self.stream.write_all(&[0u8])?;
        self.unacked = 0;
        Ok(())
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if self.poisoned {
            return Err(TryRecvError::Disconnected);
        }

        if Some(self.unacked + 1) == self.window {
            if self.send_ack().is_err() {
                return Err(TryRecvError::Empty);
            }
        }

        while self.buffer_size < mem::size_of::<u32>() {
            match self.stream
                .read(&mut self.buffer[self.buffer_size..mem::size_of::<u32>()]) {
                Ok(n) => {
                    self.buffer_size += n;
                }
                Err(_) => return Err(TryRecvError::Empty),
            }
        }

        let message_size: u32 = NetworkEndian::read_u32(&self.buffer[0..mem::size_of::<u32>()]);
        let target_buffer_size = message_size as usize + mem::size_of::<u32>();
        if self.buffer.len() < target_buffer_size {
            self.buffer.resize(target_buffer_size, 0u8);
        }

        while self.buffer_size < target_buffer_size {
            match self.stream
                .read(&mut self.buffer[self.buffer_size..target_buffer_size]) {
                Ok(n) => self.buffer_size += n,
                Err(_) => return Err(TryRecvError::Empty),
            }
        }

        self.unacked += 1;
        match bincode::deserialize(&self.buffer[mem::size_of::<u32>()..target_buffer_size]) {
            Err(_) => {
                self.poisoned = true;
                Err(TryRecvError::DeserializationError)
            }
            Ok(t) => {
                self.buffer_size = 0;
                Ok(t)
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
        self.stream.register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.stream.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.stream.deregister(poll)
    }
}

fn connect() -> (std::net::TcpStream, std::net::TcpStream) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = std::net::TcpListener::bind(&addr).unwrap();
    let rx = std::net::TcpStream::connect(&listener.local_addr().unwrap()).unwrap();
    let tx = listener.accept().unwrap().0;

    (tx, rx)
}

pub fn channel<T: Serialize>() -> (TcpSender<T>, TcpReceiver<T>)
where
    for<'de> T: Deserialize<'de>,
{
    let (tx, rx) = connect();
    let tx = TcpSender::new(tx, None).unwrap();
    let rx = TcpReceiver::new(rx).unwrap();
    (tx, rx)
}

pub fn sync_channel<T: Serialize>(size: u64) -> (TcpSender<T>, TcpReceiver<T>)
where
    for<'de> T: Deserialize<'de>,
{
    let (tx, rx) = connect();
    let tx = TcpSender::new(tx, Some(size)).unwrap();
    let rx = TcpReceiver::new(rx).unwrap();
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
    fn poll2() {
        let (mut _sender, receiver) = channel::<u32>();
        let (mut sender2, mut receiver2) = channel::<u32>();

        let t1 = thread::spawn(move || {
            let poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();
            poll.register(&receiver2, Token(1), Ready::readable(), PollOpt::level())
                .unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(1));
            assert_eq!(receiver2.recv().unwrap(), 13);
        });

        let t2 = thread::spawn(move || { sender2.send(13).unwrap(); });

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
