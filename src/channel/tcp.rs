
use std::net::TcpStream;
use std::marker::PhantomData;
use std::os::unix::io::{AsRawFd, RawFd};

use bincode::{self, Infinite};
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum Error {
    BincodeError(bincode::Error),
    Poisoned,
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::BincodeError(e)
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
    stream: TcpStream,
    window: Option<u64>,
    unacked: u64,
    poisoned: bool,

    phantom: PhantomData<T>,
}
impl<T: Serialize> TcpSender<T> {
    pub fn new(stream: TcpStream, window: Option<u64>) -> Self {
        Self {
            stream,
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
            poisoning_try!(
                self,
                bincode::deserialize_from::<_, u8, _>(&mut self.stream, Infinite)
            );
            self.unacked = 0;
        }

        self.unacked += 1;
        Ok(poisoning_try!(
            self,
            bincode::serialize_into(&mut self.stream, t, Infinite)
        ))
    }
}
impl<T> AsRawFd for TcpSender<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}


pub struct TcpReceiver<T> {
    stream: TcpStream,
    window: Option<u64>,
    unacked: u64,
    poisoned: bool,

    phantom: PhantomData<T>,
}
impl<T> TcpReceiver<T>
where
    for<'de> T: Deserialize<'de>,
{
    pub fn new(stream: TcpStream, window: Option<u64>) -> Self {
        Self {
            stream,
            window,
            unacked: 0,
            poisoned: false,
            phantom: PhantomData,
        }
    }

    pub fn recv(&mut self) -> Result<T, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }

        self.unacked += 1;
        if Some(self.unacked) == self.window {
            poisoning_try!(
                self,
                bincode::serialize_into(&mut self.stream, &0u8, Infinite)
            );
            self.unacked = 0;
        }

        Ok(poisoning_try!(
            self,
            bincode::deserialize_from::<_, T, _>(&mut self.stream, Infinite)
        ))
    }
}
impl<T> AsRawFd for TcpReceiver<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    fn connect() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let rx = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
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

}
