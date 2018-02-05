use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex};
use std::thread;

use failure::Error;
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
    fn become_leader(&self, payload_data: Vec<u8>) -> Result<Option<Epoch>, Error> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.keys.contains_key(CONTROLLER_KEY) {
            inner.keys.insert(CONTROLLER_KEY.to_owned(), payload_data);
            self.cv.notify_all();
            Ok(Some(ONLY_EPOCH))
        } else {
            Ok(None)
        }
    }

    fn get_leader(&self) -> Result<(Epoch, Vec<u8>), Error> {
        let mut inner = self.inner.lock().unwrap();
        while !inner.keys.contains_key(CONTROLLER_KEY) {
            inner = self.cv.wait(inner).unwrap();
        }
        Ok((ONLY_EPOCH, inner.keys.get(CONTROLLER_KEY).cloned().unwrap()))
    }

    fn try_get_leader(&self) -> Result<Option<(Epoch, Vec<u8>)>, Error> {
        Ok(self.inner
            .lock()
            .unwrap()
            .keys
            .get(CONTROLLER_KEY)
            .cloned()
            .map(|payload| (ONLY_EPOCH, payload)))
    }

    fn await_new_epoch(&self, epoch: Epoch) -> Result<Option<(Epoch, Vec<u8>)>, Error> {
        assert_eq!(epoch, ONLY_EPOCH);

        {
            let inner = self.inner.lock().unwrap();
            if !inner.keys.contains_key(CONTROLLER_KEY) {
                return Ok(None);
            }
        }

        // Epochs never change with a LocalAuthority, so this function should never return.
        loop {
            thread::park();
        }
    }

    fn try_read(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.keys.get(path).cloned())
    }

    fn read_modify_write<F, P, E>(&self, path: &str, mut f: F) -> Result<Result<P, E>, Error>
    where
        F: FnMut(Option<P>) -> Result<P, E>,
        P: Serialize + DeserializeOwned,
    {
        let mut inner = self.inner.lock().unwrap();
        let r = f(inner
            .keys
            .get(path)
            .map(|data| serde_json::from_slice(data).unwrap()));
        if let Ok(ref p) = r {
            inner
                .keys
                .insert(path.to_owned(), serde_json::to_vec(&p).unwrap());
        }
        Ok(r)
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
