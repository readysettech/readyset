use channel::rpc::RpcClient;

use flow::prelude::*;
use flow;
use checktable;
use backlog::{self, ReadHandle};

use std::sync::Arc;
use std::net::SocketAddr;

use arrayvec::ArrayVec;

/// A request to read a specific key.
#[derive(Serialize, Deserialize)]
pub struct ReadQuery {
    /// Where to read from
    pub target: (NodeIndex, usize),
    /// Keys to read with
    pub keys: Vec<DataType>,
    /// Whether to block of a partial reply is triggered
    pub block: bool,
}

/// The contents of a specific key
#[derive(Serialize, Deserialize)]
pub struct ReadReply(pub Vec<Result<Vec<Vec<DataType>>, ()>>);

/// Serializeable version of a `RemoteGetter`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteGetterBuilder {
    pub(crate) node: NodeIndex,
    pub(crate) shards: Vec<SocketAddr>,
}

impl RemoteGetterBuilder {
    /// Build a `RemoteGetter` out of a `RemoteGetterBuilder`
    pub fn build(self) -> RemoteGetter {
        RemoteGetter {
            node: self.node,
            shards: self.shards
                .iter()
                .map(|addr| RpcClient::connect(addr).unwrap())
                .collect(),
        }
    }
}

/// Struct to query the contents of a materialized view.
pub struct RemoteGetter {
    node: NodeIndex,
    shards: Vec<RpcClient<ReadQuery, ReadReply>>,
}

impl RemoteGetter {
    /// Query for the results for the given key, optionally blocking if it is not yet available.
    pub fn lookup(&mut self, keys: Vec<DataType>, block: bool) -> Vec<Result<Datas, ()>> {
        if self.shards.len() == 1 {
            let ReadReply(rows) = self.shards[0]
                .send(&ReadQuery {
                    target: (self.node, 0),
                    keys,
                    block,
                })
                .unwrap();
            rows
        } else {
            let mut shard_queries = vec![Vec::new(); self.shards.len()];
            for key in keys {
                let shard = ::shard_by(&key, self.shards.len());
                shard_queries[shard].push(key);
            }

            shard_queries
                .into_iter()
                .enumerate()
                .flat_map(|(shard, keys)| {
                    let ReadReply(rows) = self.shards[shard]
                        .send(&ReadQuery {
                            target: (self.node, shard),
                            keys,
                            block,
                        })
                        .unwrap();
                    rows
                })
                .collect()
        }
    }
}

/// A handle for looking up results in a materialized view.
pub struct Getter {
    pub(crate) generator: Option<checktable::TokenGenerator>,
    pub(crate) handle: backlog::ReadHandle,
    last_ts: i64,
}

impl Getter {
    pub(crate) fn new(
        node: NodeIndex,
        sharded: bool,
        readers: &flow::Readers,
        ingredients: &Graph,
    ) -> Option<Self> {
        let rh = if sharded {
            let vr = readers.lock().unwrap();

            let mut array = ArrayVec::new();
            for shard in 0..::SHARDS {
                match vr.get(&(node, shard)).cloned() {
                    Some(rh) => array.push(Some(rh)),
                    None => return None,
                }
            }
            ReadHandle::Sharded(array)
        } else {
            let vr = readers.lock().unwrap();
            match vr.get(&(node, 0)).cloned() {
                Some(rh) => ReadHandle::Singleton(Some(rh)),
                None => return None,
            }
        };

        let gen = ingredients[node]
            .with_reader(|r| r)
            .and_then(|r| r.token_generator().cloned());
        assert_eq!(ingredients[node].is_transactional(), gen.is_some());
        Some(Getter {
            generator: gen,
            handle: rh,
            last_ts: i64::min_value(),
        })
    }

    /// Returns true if this getter supports transactional reads.
    pub fn supports_transactions(&self) -> bool {
        self.generator.is_some()
    }

    /// Query for the results for the given key, and apply the given callback to matching rows.
    ///
    /// If `block` is `true`, this function will block if the results for the given key are not yet
    /// available.
    ///
    /// If you need to clone values out of the returned rows, make sure to use
    /// `DataType::deep_clone` to avoid contention on internally de-duplicated strings!
    pub fn lookup_map<F, T>(&self, q: &DataType, mut f: F, block: bool) -> Result<Option<T>, ()>
    where
        F: FnMut(&[Arc<Vec<DataType>>]) -> T,
    {
        self.handle.find_and(q, |rs| f(&rs[..]), block).map(|r| r.0)
    }

    /// Query for the results for the given key, optionally blocking if it is not yet available.
    pub fn lookup(&self, q: &DataType, block: bool) -> Result<Datas, ()> {
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

    /// Transactionally query for the given key, blocking if it is not yet available.
    pub fn transactional_lookup(&mut self, q: &DataType) -> Result<(Datas, checktable::Token), ()> {
        match self.generator {
            None => Err(()),
            Some(ref g) => {
                loop {
                    let res = self.handle.find_and(
                        q,
                        |rs| {
                            rs.into_iter()
                                .map(|v| (&**v).into_iter().map(|v| v.deep_clone()).collect())
                                .collect()
                        },
                        true,
                    );
                    match res {
                        Ok((_, ts)) if ts < self.last_ts => {
                            // we must have read from a different shard that is not yet up-to-date
                            // to our last read. this is *extremely* unlikely: you would have to
                            // issue two reads to different shards *between* the barrier and swap
                            // inside Reader nodes, which is only a span of a handful of
                            // instructions. But it is *possible*.
                        }
                        Ok((res, ts)) => {
                            self.last_ts = ts;
                            let token = g.generate(ts, q.clone());
                            break Ok((res.unwrap_or_else(Vec::new), token));
                        }
                        Err(e) => break Err(e),
                    }
                }
            }
        }
    }
}
