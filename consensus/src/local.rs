use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex};
use std::thread;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;

use Authority;
use Epoch;
use CONTROLLER_KEY;

/// Only Epoch there will ever be for a LocalAuthority.
const ONLY_EPOCH: Epoch = Epoch(1);

#[derive(Default)]
struct LocalAuthorityInner {
    keys: BTreeMap<String, Vec<u8>>,
}

pub struct LocalAuthority {
    inner: Mutex<LocalAuthorityInner>,
    cv: Condvar,
}

impl LocalAuthority {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LocalAuthorityInner::default()),
            cv: Condvar::new(),
        }
    }

    /// Get the only epoch that can ever exist for a local authority.
    pub fn get_epoch() -> Epoch {
        ONLY_EPOCH
    }
}
impl Authority for LocalAuthority {
    fn become_leader(&self, payload_data: Vec<u8>) -> Epoch {
        {
            let mut inner = self.inner.lock().unwrap();
            if !inner.keys.contains_key(CONTROLLER_KEY) {
                inner.keys.insert(CONTROLLER_KEY.to_owned(), payload_data);
                self.cv.notify_all();
                return ONLY_EPOCH;
            }
        }

        loop {
            thread::park();
        }
    }

    fn get_leader(&self) -> (Epoch, Vec<u8>) {
        let mut inner = self.inner.lock().unwrap();
        while !inner.keys.contains_key(CONTROLLER_KEY) {
            inner = self.cv.wait(inner).unwrap();
        }
        (ONLY_EPOCH, inner.keys.get(CONTROLLER_KEY).cloned().unwrap())
    }

    fn await_new_epoch(&self, _: Epoch) -> Epoch {
        loop {
            thread::park();
        }
    }

    fn try_read(&self, path: &str) -> Option<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        inner.keys.get(path).cloned()
    }

    fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<P, E>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned,
    {
        let mut inner = self.inner.lock().unwrap();
        let r = f(
            inner
                .keys
                .get(path)
                .map(|data| serde_json::from_slice(data).unwrap()),
        );
        if let Ok(ref p) = r {
            inner
                .keys
                .insert(path.to_owned(), serde_json::to_vec(&p).unwrap());
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::sync::Arc;

    #[test]
    fn it_works() {
        let authority = Arc::new(LocalAuthority::new());
        assert!(authority.try_read(CONTROLLER_KEY).is_none());
        assert!(authority.try_read("/a").is_none());
        assert_eq!(
            authority.read_modify_write("/a", |arg: Option<u32>| -> Result<u32, u32> {
                assert!(arg.is_none());
                Ok(12)
            }),
            Ok(12)
        );
        assert_eq!(authority.try_read("/a"), Some("12".bytes().collect()));
        assert_eq!(authority.become_leader(vec![15]), Epoch(1));
        assert_eq!(authority.get_leader(), (Epoch(1), vec![15]));
        {
            let authority = authority.clone();
            thread::spawn(move || authority.become_leader(vec![20]));
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(authority.get_leader(), (Epoch(1), vec![15]));
    }
}
