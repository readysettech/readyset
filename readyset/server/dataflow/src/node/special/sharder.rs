use readyset::KeyComparison;
use serde::{Deserialize, Serialize};
use vec_map::VecMap;

use crate::payload;
use crate::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct Sharder {
    #[serde(skip)]
    txs: Vec<(LocalNodeIndex, ReplicaAddr)>,
    #[serde(skip)]
    sharded: VecMap<Box<Packet>>,
    shard_by: usize,
}

impl Clone for Sharder {
    fn clone(&self) -> Self {
        debug_assert!(self.txs.is_empty());

        Sharder {
            txs: Vec::new(),
            sharded: Default::default(),
            shard_by: self.shard_by,
        }
    }
}

impl Sharder {
    pub fn new(by: usize) -> Self {
        Self {
            txs: Default::default(),
            shard_by: by,
            sharded: VecMap::default(),
        }
    }

    #[must_use]
    pub fn take(&mut self) -> Self {
        let txs = std::mem::take(&mut self.txs);
        Self {
            txs,
            sharded: VecMap::default(),
            shard_by: self.shard_by,
        }
    }

    pub fn add_sharded_child(&mut self, dst: LocalNodeIndex, txs: Vec<ReplicaAddr>) {
        debug_assert_eq!(self.txs.len(), 0);
        // TODO: add support for "shared" sharder?
        for tx in txs {
            self.txs.push((dst, tx));
        }
    }

    pub fn sharded_by(&self) -> usize {
        self.shard_by
    }

    #[inline]
    fn to_shard(&self, r: &Record) -> usize {
        self.shard(&r[self.shard_by])
    }

    #[inline]
    fn shard(&self, dt: &DataType) -> usize {
        crate::shard_by(dt, self.txs.len())
    }

    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        index: LocalNodeIndex,
        is_sharded: bool,
        is_last_sharder_for_tag: Option<bool>,
        output: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        // we need to shard the records inside `m` by their key,
        let mut m = m.take().unwrap();
        for record in m.take_data() {
            let shard = self.to_shard(&record);
            let p = self
                .sharded
                .entry(shard)
                .or_insert_with(|| Box::new(m.clone_data()));
            p.map_data(|rs| rs.push(record));
        }

        enum Destination {
            All,
            One(usize),
            Any,
        }

        let mut dest = Destination::Any;
        if let Packet::ReplayPiece {
            context: payload::ReplayPieceContext::Regular { last: true },
            ..
        } = *m
        {
            // this is the last replay piece for a full replay
            // we need to make sure it gets to every shard so they know to ready the node
            dest = Destination::All;
        } else if let Packet::ReplayPiece {
            context:
                payload::ReplayPieceContext::Partial {
                    requesting_shard, ..
                },
            ..
        } = *m
        {
            if let Some(true) = is_last_sharder_for_tag {
                // we are the last sharder and the replay target is sharded
                // so we need to make sure only that shard gets the replay response,
                // since the others aren't expecting it.
                dest = Destination::One(requesting_shard);
            } else {
                // either, we are not the last sharder on the replay path
                // or, the ultimate target of the replay is not sharded.
                // in either case, we need to forward to all shards, since there will
                // be a shard merger below us that expects a message from all shards.
                dest = Destination::All;
            }
        } else {
            invariant!(is_last_sharder_for_tag.is_none());
        }

        match dest {
            Destination::All => {
                // ensure that every shard gets a packet
                // note that m has no data, so m.clone_data() is empty
                for shard in 0..self.txs.len() {
                    self.sharded
                        .entry(shard)
                        .or_insert_with(|| Box::new(m.clone_data()));
                }
            }
            Destination::One(shard) => {
                // ensure that the target shard gets a packet
                self.sharded
                    .entry(shard)
                    .or_insert_with(|| Box::new(m.clone_data()));
                // and that no-one else does
                self.sharded.retain(|k, _| k == shard);
            }
            Destination::Any => {}
        }

        if is_sharded {
            // FIXME: we don't know how many shards in the destination domain our sibling Sharders
            // sent to, so we don't know what to put here. we *could* put self.txs.len() and send
            // empty messages to all other shards, which is probably pretty sensible, but that only
            // solves half the problem. the destination shard domains will then recieve *multiple*
            // replay pieces for each incoming replay piece, and needs to combine them somehow.
            // it's unclear how we do that.
            unsupported!("we don't know how to shard a shard");
        }

        for (i, &mut (dst, addr)) in self.txs.iter_mut().enumerate() {
            if let Some(mut shard) = self.sharded.remove(i) {
                shard.link_mut().src = index;
                shard.link_mut().dst = dst;
                output.send(addr, shard);
            }
        }

        Ok(())
    }

    #[allow(clippy::unreachable)]
    pub fn process_eviction(
        &mut self,
        key_columns: &[usize],
        tag: Tag,
        keys: &[KeyComparison],
        src: LocalNodeIndex,
        is_sharded: bool,
        output: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        invariant!(!is_sharded);

        if key_columns.len() == 1 && key_columns[0] == self.shard_by {
            // Send only to the shards that must evict something.
            for key in keys {
                for shard in key.shard_keys(self.txs.len()) {
                    let dst = self.txs[shard].0;
                    let p = self.sharded.entry(shard).or_insert_with(|| {
                        Box::new(Packet::EvictKeys {
                            link: Link { src, dst },
                            keys: Vec::new(),
                            tag,
                        })
                    });
                    match **p {
                        Packet::EvictKeys { ref mut keys, .. } => keys.push(key.clone()),
                        _ => {
                            // TODO: Scoped for a future refactor:
                            // https://readysettech.atlassian.net/browse/ENG-455
                            unreachable!("received a non EvictKey packed in process_eviction")
                        }
                    }
                }
            }

            for (i, &mut (_, addr)) in self.txs.iter_mut().enumerate() {
                if let Some(shard) = self.sharded.remove(i) {
                    output.send(addr, shard);
                }
            }
        } else {
            invariant_eq!(!key_columns.len(), 0);
            invariant!(!key_columns.contains(&self.shard_by));

            // send to all shards
            for &mut (dst, addr) in self.txs.iter_mut() {
                output.send(
                    addr,
                    Box::new(Packet::EvictKeys {
                        link: Link { src, dst },
                        keys: keys.to_vec(),
                        tag,
                    }),
                )
            }
        }

        Ok(())
    }
}
