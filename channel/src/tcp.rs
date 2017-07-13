
use std;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::mem;

use bincode::{self, Infinite};
use bufstream::BufStream;
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
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
    pub fn new(stream: std::net::TcpStream, window: Option<u64>) -> Self {
        Self {
            stream: BufStream::new(stream),
            window,
            unacked: 0,
            poisoned: false,
            phantom: PhantomData,
        }
    }

    pub fn send(&mut self, t: &T) -> Result<(), Error> {
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
    pub fn new(stream: mio::net::TcpStream, window: Option<u64>) -> Self {
        if let Some(w) = window {
            assert!(w > 0);
        }

        Self {
            stream,
            window,
            unacked: 0,
            buffer: vec![0; 1024],
            buffer_size: 0,
            poisoned: false,
            phantom: PhantomData,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::net::SocketAddr;
    use mio::Events;
    use std::net::TcpListener;

    fn connect() -> (std::net::TcpStream, mio::net::TcpStream) {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();
        let rx = mio::net::TcpStream::connect(&listener.local_addr().unwrap()).unwrap();
        let tx = listener.accept().unwrap().0;

        (tx, rx)
    }

    #[test]
    fn unbounded() {
        let (tx, rx) = connect();
        let mut sender = TcpSender::<u32>::new(tx, None);
        let mut receiver = TcpReceiver::<u32>::new(rx, None);

        sender.send(&12).unwrap();
        assert_eq!(receiver.recv().unwrap(), 12);

        sender.send(&65).unwrap();
        sender.send(&13).unwrap();
        assert_eq!(receiver.recv().unwrap(), 65);
        assert_eq!(receiver.recv().unwrap(), 13);
    }

    #[test]
    fn bounded() {
        let (tx, rx) = connect();
        let mut sender = TcpSender::<u32>::new(tx, Some(2));
        let mut receiver = TcpReceiver::<u32>::new(rx, Some(2));

        sender.send(&12).unwrap();
        sender.send(&65).unwrap();
        assert_eq!(receiver.recv().unwrap(), 12);
        assert_eq!(receiver.recv().unwrap(), 65);

        sender.send(&13).unwrap();
        assert_eq!(receiver.recv().unwrap(), 13);
    }

    #[test]
    fn bounded_multithread() {
        let (tx, rx) = connect();
        let mut sender = TcpSender::<u32>::new(tx, Some(2));
        let mut receiver = TcpReceiver::<u32>::new(rx, Some(2));

        let t1 = thread::spawn(move || {
            sender.send(&12).unwrap();
            sender.send(&65).unwrap();
            sender.send(&13).unwrap();
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
        let (tx, rx) = connect();
        let mut sender = TcpSender::<u32>::new(tx, Some(2));
        let mut receiver = TcpReceiver::<u32>::new(rx, Some(2));

        let t1 = thread::spawn(move || {
            sender.send(&12).unwrap();
            sender.send(&65).unwrap();
            sender.send(&13).unwrap();
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
        let (tx, rx) = connect();
        let _sender = TcpSender::<u32>::new(tx, None);
        let receiver = TcpReceiver::<u32>::new(rx, None);

        let (tx, rx) = connect();
        let mut sender2 = TcpSender::<u32>::new(tx, None);
        let mut receiver2 = TcpReceiver::<u32>::new(rx, None);

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

        let t2 = thread::spawn(move || { sender2.send(&13).unwrap(); });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn ping_pong() {
        let (tx, rx) = connect();
        let mut sender = TcpSender::<u32>::new(tx, Some(1));
        let mut receiver = TcpReceiver::<u32>::new(rx, Some(1));

        let (tx, rx) = connect();
        let mut sender2 = TcpSender::<u32>::new(tx, Some(1));
        let mut receiver2 = TcpReceiver::<u32>::new(rx, Some(1));

        let t1 = thread::spawn(move || {
            let poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 15);

            sender2.send(&12).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver.recv().unwrap(), 54);

            sender2.send(&65).unwrap();
        });

        let t2 = thread::spawn(move || {
            let poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(128);
            poll.register(&receiver2, Token(0), Ready::readable(), PollOpt::level())
                .unwrap();

            sender.send(&15).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver2.recv().unwrap(), 12);

            sender.send(&54).unwrap();

            poll.poll(&mut events, None).unwrap();
            assert_eq!(events.iter().next().unwrap().token(), Token(0));
            assert_eq!(receiver2.recv().unwrap(), 65);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
