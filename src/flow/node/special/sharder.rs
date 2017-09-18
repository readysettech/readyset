use flow::prelude::*;
use flow::payload;
use vec_map::VecMap;
use channel::ChannelSender;
use std::collections::HashSet;

#[derive(Serialize, Deserialize)]
pub struct Sharder {
    txs: Vec<(LocalNodeIndex, ChannelSender<Box<Packet>>)>,
    sharded: VecMap<Box<Packet>>,
    shard_by: usize,
}

impl Clone for Sharder {
    fn clone(&self) -> Self {
        Sharder {
            txs: self.txs.clone(),
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

    pub fn take(&mut self) -> Self {
        use std::mem;
        let txs = mem::replace(&mut self.txs, Vec::new());
        Self {
            txs: txs,
            sharded: VecMap::default(),
            shard_by: self.shard_by,
        }
    }

    pub fn add_sharded_child(&mut self, dst: LocalNodeIndex, txs: Vec<ChannelSender<Box<Packet>>>) {
        assert_eq!(self.txs.len(), 0);
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
        ::shard_by(dt, self.txs.len())
    }

    pub fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        index: LocalNodeIndex,
        is_sharded: bool,
    ) {
        // we need to shard the records inside `m` by their key,
        let mut m = m.take().unwrap();

        if m.tag().is_some() {
            // this is a replay packet, which we need to make sure we route correctly
            if let box Packet::ReplayPiece {
                context: payload::ReplayPieceContext::Partial { .. },
                ..
            } = m
            {
                // we need to send one message to each shard in for_keys
                let shards = if let box Packet::ReplayPiece {
                    context: payload::ReplayPieceContext::Partial {
                        ref mut for_keys, ..
                    },
                    ..
                } = m
                {
                    let keys = for_keys.len();
                    for_keys
                        .drain()
                        .map(|key| {
                            assert_eq!(key.len(), 1);
                            let shard = self.shard(&key[0]);
                            (shard, key)
                        })
                        .fold(VecMap::new(), |mut hm, (shard, key)| {
                            hm.entry(shard).or_insert_with(|| HashSet::with_capacity(keys)).insert(key);
                            hm
                        })
                } else {
                    unreachable!()
                };

                let records = m.take_data();
                let mut shards: VecMap<_> = shards
                    .into_iter()
                    .map(|(shard, keys)| {
                        let mut p = m.clone_data();
                        if let Packet::ReplayPiece {
                            ref mut nshards,
                            context: payload::ReplayPieceContext::Partial {
                                ref mut for_keys, ..
                            },
                            ..
                        } = p
                        {
                            *nshards = 1;
                            *for_keys = keys;
                        } else {
                            unreachable!();
                        }

                        (shard, box p)
                    })
                    .collect();

                for record in records {
                    let shard = self.to_shard(&record);
                    shards[shard].map_data(|rs| rs.push(record));
                }

                for (shard, p) in shards {
                    let tx = &mut self.txs[shard];
                    m.link_mut().src = index;
                    m.link_mut().dst = tx.0;
                    if tx.1.send(p).is_err() {
                        // we must be shutting down...
                    }
                }
                return;
            }

            // fall-through for regular replays, because they aren't really special in any way.
        }

        for record in m.take_data() {
            let shard = self.to_shard(&record);
            let p = self.sharded
                .entry(shard)
                .or_insert_with(|| box m.clone_data());
            p.map_data(|rs| rs.push(record));
        }

        let mut force_all = false;
        if let Packet::ReplayPiece {
            context: payload::ReplayPieceContext::Regular { last: true },
            ..
        } = *m
        {
            // this is the last replay piece for a full replay
            // we need to make sure it gets to every shard so they know to ready the node
            force_all = true;
        }
        if let Packet::Transaction { .. } = *m {
            // transactions (currently) need to reach all shards so they know they can progress
            force_all = true;
        }
        if force_all {
            for shard in 0..self.txs.len() {
                self.sharded
                    .entry(shard)
                    .or_insert_with(|| box m.clone_data());
            }
        }

        if is_sharded {
            // FIXME: we don't know how many shards in the destination domain our sibling Sharders
            // sent to, so we don't know what to put here. we *could* put self.txs.len() and send
            // empty messages to all other shards, which is probably pretty sensible, but that only
            // solves half the problem. the destination shard domains will then recieve *multiple*
            // replay pieces for each incoming replay piece, and needs to combine them somehow.
            // it's unclear how we do that.
            unimplemented!();
        }
        if let Packet::ReplayPiece {
            ref mut nshards, ..
        } = *m
        {
            *nshards = self.sharded.len();
        }

        for (i, &mut (dst, ref mut tx)) in self.txs.iter_mut().enumerate() {
            if let Some(mut shard) = self.sharded.remove(i) {
                shard.link_mut().src = index.into();
                shard.link_mut().dst = dst;

                if tx.send(shard).is_err() {
                    // we must be shutting down...
                    break;
                }
            }
        }
    }
}
