use flow::prelude::*;
use flow;
use checktable;
use backlog;
use std::sync::Arc;

/// A handle for looking up results in a materialized view.
pub struct Getter {
    pub(crate) generator: Option<checktable::TokenGenerator>,
    pub(crate) handle: backlog::ReadHandle,
    last_ts: i64,
}

impl Getter {
    pub(crate) fn new(
        node: NodeIndex,
        readers: &flow::Readers,
        ingredients: &Graph,
    ) -> Option<Self> {
        {
            let vr = readers.lock().unwrap();
            vr.get(&node).cloned()
        }.map(move |rh| {
            let gen = ingredients[node]
                .with_reader(|r| r)
                .and_then(|r| r.token_generator().cloned());
            assert_eq!(ingredients[node].is_transactional(), gen.is_some());
            Getter {
                generator: gen,
                handle: rh,
                last_ts: i64::min_value(),
            }
        })
    }

    /// Returns the number of populated keys
    pub fn len(&self) -> usize {
        self.handle.len()
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
