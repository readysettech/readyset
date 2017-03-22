
use memcached;
use memcached::proto::{Operation, ProtoType};
#[cfg(test)]
use memcached::proto::MemCachedResult;

use rustc_serialize::json::{ToJson, Json};

use flow::prelude::*;

pub struct Memcache(memcached::Client);
unsafe impl Send for Memcache {}

/// A node that pushes updates to an external datastore. Currently only Memcached is supported.
pub struct Hook {
    client: Memcache,
    key_columns: Vec<usize>,

    state: State,
}

impl Hook {
    /// Create a new Hook which is connected to some number of Memcached servers
    ///
    /// This function accepts multiple servers, servers information should be represented as a array
    /// of tuples in this form
    ///
    /// `(address, weight)`.
    pub fn new(servers: &[(&str, usize)], key_columns: Vec<usize>) -> Option<Self> {
        let client = memcached::Client::connect(&servers, ProtoType::Binary);
        if client.is_err() {
            return None;
        }

        let mut s = State::default();
        s.add_key(&key_columns[..]);

        Some(Self {
                 client: Memcache(client.unwrap()),
                 key_columns: key_columns,
                 state: s,
             })
    }

    /// Push the relevant record updates to Memcached.
    pub fn on_input(&mut self, records: Records) {
        // Update materialized state
        for rec in records.iter() {
            match rec {
                &Record::Positive(ref r) => self.state.insert(r.clone()),
                &Record::Negative(ref r) => self.state.remove(r),
                &Record::DeleteRequest(..) => unreachable!(),
            }
        }

        // Extract modified keys
        let mut modified_keys: Vec<_> = records.into_iter()
            .map(|rec| match rec {
                     Record::Positive(a) |
                     Record::Negative(a) => {
                         a.iter()
                             .enumerate()
                             .filter_map(|(i, v)| if self.key_columns.iter().any(|col| {
                                                                                     col == &i
                                                                                 }) {
                                             Some(v)
                                         } else {
                                             None
                                         })
                             .cloned()
                             .collect::<Vec<_>>()
                     }
                     Record::DeleteRequest(..) => unreachable!(),
                 })
            .collect();

        // Remove duplicates
        modified_keys.sort();
        modified_keys.dedup();

        // Push to Memcached
        for key in modified_keys {
            let rows = self.state.lookup(&self.key_columns[..], &KeyType::from(&key[..]));
            let k = key.to_json().to_string();
            let v = Json::Array(rows.into_iter()
                                    .map(|row| {
                                             Json::Array(row.iter().map(|c| c.to_json()).collect())
                                         })
                                    .collect())
                    .to_string();
            let flags = 0xdeadbeef;
            (self.client.0).set(k.as_bytes(), v.as_bytes(), flags, 0).unwrap();
        }
    }

    #[cfg(test)]
    pub fn get_row(&mut self, key: Vec<DataType>) -> MemCachedResult<(Vec<u8>, u32)> {
        self.client.0.get(key.to_json().to_string().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // #[ignore]
    fn it_works() {
        let mut h = Hook::new(&[("tcp://127.0.0.1:11211", 1)], vec![0]).unwrap();

        // Insert a row
        h.on_input(vec![vec![2.into(), 2.into()]].into());
        let row = h.get_row(vec![2.into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[[2,2]]");

        // Delete it
        h.on_input(vec![(vec![2.into(), 2.into()], false)].into());
        let row = h.get_row(vec![2.into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[]");

        // Insert two more rows
        h.on_input(vec![vec![2.into(), 3.into()], vec![2.into(), 4.into()]].into());
        let row = h.get_row(vec![2.into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[[2,3],[2,4]]");

        // And a row for a different key
        h.on_input(vec![vec!["abc".into(), 123.into()]].into());
        let row = h.get_row(vec!["abc".into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[[\"abc\",123]]");
    }

    #[test]
    // #[ignore]
    fn it_works_multikey() {
        let mut h = Hook::new(&[("tcp://127.0.0.1:11211", 1)], vec![0, 2]).unwrap();

        // Insert a row
        h.on_input(vec![vec![2.into(), 5.into(), 3.into()]].into());
        let row = h.get_row(vec![2.into(), 3.into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[[2,5,3]]");

        // Delete it
        h.on_input(vec![(vec![2.into(), 5.into(), 3.into()], false)].into());
        let row = h.get_row(vec![2.into(), 3.into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[]");

        // Insert two more rows
        h.on_input(vec![vec![2.into(), 6.into(), 3.into()],
                        vec![2.into(), "xyz".into(), 3.into()]]
                           .into());
        let row = h.get_row(vec![2.into(), 3.into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[[2,6,3],[2,\"xyz\",3]]");

        // And a row for a different key
        h.on_input(vec![vec![2.into(), "abc".into(), "def".into()]].into());
        let row = h.get_row(vec![2.into(), "def".into()]).unwrap().0;
        assert_eq!(String::from_utf8(row).unwrap(), "[[2,\"abc\",\"def\"]]");
    }
}
