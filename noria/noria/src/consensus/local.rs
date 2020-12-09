use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex};

use failure::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::Authority;
use super::Epoch;
use super::CONTROLLER_KEY;

struct LocalAuthorityInner {
    keys: BTreeMap<String, Vec<u8>>,
    epoch: Epoch,
}

pub struct LocalAuthority {
    inner: Mutex<LocalAuthorityInner>,
    cv: Condvar,
}

impl Default for LocalAuthority {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalAuthority {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LocalAuthorityInner {
                keys: BTreeMap::default(),
                epoch: Epoch(0),
            }),
            cv: Condvar::new(),
        }
    }
}
impl Authority for LocalAuthority {
    fn become_leader(&self, payload_data: Vec<u8>) -> Result<Option<Epoch>, Error> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.keys.contains_key(CONTROLLER_KEY) {
            inner.keys.insert(CONTROLLER_KEY.to_owned(), payload_data);
            self.cv.notify_all();
            Ok(Some(inner.epoch))
        } else {
            Ok(None)
        }
    }

    fn surrender_leadership(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        assert!(inner.keys.remove(CONTROLLER_KEY).is_some());
        inner.epoch = Epoch(inner.epoch.0 + 1);
        self.cv.notify_all();
        Ok(())
    }

    fn get_leader(&self) -> Result<(Epoch, Vec<u8>), Error> {
        let mut inner = self.inner.lock().unwrap();
        while !inner.keys.contains_key(CONTROLLER_KEY) {
            inner = self.cv.wait(inner).unwrap();
        }
        Ok((
            inner.epoch,
            inner.keys.get(CONTROLLER_KEY).cloned().unwrap(),
        ))
    }

    fn try_get_leader(&self) -> Result<Option<(Epoch, Vec<u8>)>, Error> {
        let inner = self.inner.lock().unwrap();

        Ok(inner
            .keys
            .get(CONTROLLER_KEY)
            .cloned()
            .map(|payload| (inner.epoch, payload)))
    }

    fn await_new_epoch(&self, epoch: Epoch) -> Result<Option<(Epoch, Vec<u8>)>, Error> {
        let mut inner = self.inner.lock().unwrap();
        while inner.epoch == epoch && inner.keys.contains_key(CONTROLLER_KEY) {
            inner = self.cv.wait(inner).unwrap();
        }

        Ok(inner
            .keys
            .get(CONTROLLER_KEY)
            .cloned()
            .map(|k| (inner.epoch, k)))
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
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn it_works() {
        let authority = Arc::new(LocalAuthority::new());
        assert!(authority.try_read(CONTROLLER_KEY).unwrap().is_none());
        assert!(authority.try_read("/a").unwrap().is_none());
        assert_eq!(
            authority
                .read_modify_write("/a", |arg: Option<u32>| -> Result<u32, u32> {
                    assert!(arg.is_none());
                    Ok(12)
                })
                .unwrap(),
            Ok(12)
        );
        assert_eq!(
            authority.try_read("/a").unwrap(),
            Some("12".bytes().collect())
        );
        assert_eq!(authority.become_leader(vec![15]).unwrap(), Some(Epoch(0)));
        assert_eq!(authority.get_leader().unwrap(), (Epoch(0), vec![15]));
        {
            let authority = authority.clone();
            thread::spawn(move || authority.become_leader(vec![20]).unwrap());
        }
        thread::sleep(Duration::from_millis(100));
        assert_eq!(authority.get_leader().unwrap(), (Epoch(0), vec![15]));
    }
}
