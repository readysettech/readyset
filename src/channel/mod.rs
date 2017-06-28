
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::sync::mpsc::{self, SendError};

use serde::{Serialize, Serializer, Deserialize, Deserializer};

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
}

struct ChannelCoordinatorInner<K: Eq + Hash + Clone, P> {
    txs: HashMap<K, ChannelSender<P>>,
    input_txs: HashMap<K, ChannelSender<P>>,
}

pub struct ChannelCoordinator<K: Eq + Hash + Clone, P> {
    inner: Mutex<ChannelCoordinatorInner<K, P>>,
}

impl<K: Eq + Hash + Clone, P> ChannelCoordinator<K, P> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ChannelCoordinatorInner {
                txs: HashMap::new(),
                input_txs: HashMap::new(),
            }),
        }
    }

    pub fn insert_tx(&self, key: K, tx: ChannelSender<P>, input_tx: ChannelSender<P>) {
        let mut inner = self.inner.lock().unwrap();
        inner.txs.insert(key.clone(), tx);
        inner.input_txs.insert(key, input_tx);
    }

    pub fn get_tx(&self, key: &K) -> Option<ChannelSender<P>> {
        self.inner.lock().unwrap().txs.get(key).cloned()
    }

    pub fn get_input_tx(&self, key: &K) -> Option<ChannelSender<P>> {
        self.inner.lock().unwrap().input_txs.get(key).cloned()
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.txs.clear();
        inner.input_txs.clear();
    }
}
