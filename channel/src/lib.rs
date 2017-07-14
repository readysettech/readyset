#![feature(custom_attribute)]

extern crate bincode;
extern crate bufstream;
extern crate byteorder;
extern crate mio;
#[macro_use]
extern crate serde_derive;
extern crate serde;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::sync::mpsc::{self, SendError};
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

use serde::{Serialize, Serializer, Deserialize, Deserializer};

pub mod tcp;

pub use tcp::{channel, sync_channel, TcpSender, TcpReceiver};

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

    pub fn from_sync(sync: mpsc::SyncSender<T>) -> Self {
        ChannelSender::LocalSync(sync)
    }
}

mod panic_serialize {
    use serde::{Serializer, Deserializer};
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
pub struct STcpSender<T>(
    #[serde(with = "panic_serialize")]
    pub TcpSender<T>
);

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
    addrs: HashMap<K, SocketAddr>,
}

pub struct ChannelCoordinator<K: Eq + Hash + Clone> {
    inner: Mutex<ChannelCoordinatorInner<K>>,
}

impl<K: Eq + Hash + Clone> ChannelCoordinator<K> {
    pub fn new() -> Self {
        Self { inner: Mutex::new(ChannelCoordinatorInner { addrs: HashMap::new() }) }
    }

    pub fn insert_addr(&self, key: K, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.addrs.insert(key, addr);
    }

    fn get_sized_tx<T: Serialize>(&self, key: &K, size: Option<u32>) -> Option<TcpSender<T>> {
        let addr = { self.inner.lock().unwrap().addrs.get(key).cloned() };
        addr.and_then(|addr| TcpSender::connect(&addr, size).ok())
    }

    pub fn get_tx<T: Serialize>(&self, key: &K) -> Option<TcpSender<T>> {
        self.get_sized_tx(key, None)
    }

    pub fn get_input_tx<T: Serialize>(&self, key: &K) -> Option<TcpSender<T>> {
        self.get_sized_tx(key, None)
    }

    pub fn get_unbounded_tx<T: Serialize>(&self, key: &K) -> Option<TcpSender<T>> {
        self.get_sized_tx(key, None)
    }
}
