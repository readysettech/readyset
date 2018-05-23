use dataflow::backlog::{self, ReadHandle};
use dataflow::prelude::*;
use dataflow::Readers;

/// A handle for looking up results in a materialized view.
pub struct Getter {
    pub(crate) handle: backlog::ReadHandle,
}

#[allow(unused)]
impl Getter {
    pub(crate) fn new(
        node: NodeIndex,
        sharded: bool,
        readers: &Readers,
        ingredients: &Graph,
    ) -> Option<Self> {
        let rh = if sharded {
            let vr = readers.lock().unwrap();

            let shards = ingredients[node].sharded_by().shards();
            let mut getters = Vec::with_capacity(shards);
            for shard in 0..shards {
                match vr.get(&(node, shard)).cloned() {
                    Some(rh) => getters.push(Some(rh)),
                    None => return None,
                }
            }
            ReadHandle::Sharded(getters)
        } else {
            let vr = readers.lock().unwrap();
            match vr.get(&(node, 0)).cloned() {
                Some(rh) => ReadHandle::Singleton(Some(rh)),
                None => return None,
            }
        };

        Some(Getter { handle: rh })
    }

    /// Returns the number of populated keys
    pub fn len(&self) -> usize {
        self.handle.len()
    }

    /// Query for the results for the given key, and apply the given callback to matching rows.
    ///
    /// If `block` is `true`, this function will block if the results for the given key are not yet
    /// available.
    ///
    /// If you need to clone values out of the returned rows, make sure to use
    /// `DataType::deep_clone` to avoid contention on internally de-duplicated strings!
    pub fn lookup_map<F, T>(&self, q: &[DataType], mut f: F, block: bool) -> Result<Option<T>, ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        self.handle.find_and(q, |rs| f(&rs[..]), block).map(|r| r.0)
    }

    /// Query for the results for the given key, optionally blocking if it is not yet available.
    pub fn lookup(&self, q: &[DataType], block: bool) -> Result<Datas, ()> {
        self.lookup_map(
            q,
            |rs| {
                rs.into_iter()
                    .map(|r| r.iter().map(|v| v.deep_clone()).collect())
                    .collect()
            },
            block,
        ).map(|r| r.unwrap_or_else(Vec::new))
    }
}
