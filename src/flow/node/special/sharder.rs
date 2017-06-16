use std::sync::mpsc;
use flow::prelude::*;
use flow::payload;
use vec_map::VecMap;

pub struct Sharder {
    txs: Vec<(LocalNodeIndex, mpsc::SyncSender<Box<Packet>>)>,
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

    pub fn add_sharded_child(
        &mut self,
        dst: LocalNodeIndex,
        txs: Vec<mpsc::SyncSender<Box<Packet>>>,
    ) {
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
            // full replays need to be forwarded to all shards
            if let Packet::FullReplay { .. } = *m {
                let m = *m;
                if let Packet::FullReplay { tag, state, .. } = m {
                    for (_, &mut (dst, ref mut tx)) in self.txs.iter_mut().enumerate() {
                        let m = box Packet::FullReplay {
                            link: Link::new(index, dst),
                            tag: tag,
                            state: state.clone(),
                        };

                        if tx.send(m).is_err() {
                            // we must be shutting down...
                            break;
                        }
                    }
                } else {
                    unreachable!();
                }
                return;
            }

            if let box Packet::ReplayPiece {
                context: payload::ReplayPieceContext::Partial { .. }, ..
            } = m
            {
                // we only need to send this to the shard responsible for the key being replayed!
                // ugh, I'm sad about this double destruct, but it's necessary for borrowing :(
                let shard = if let box Packet::ReplayPiece {
                    context: payload::ReplayPieceContext::Partial { ref for_key, .. }, ..
                } = m
                {
                    assert_eq!(for_key.len(), 1);
                    self.shard(&for_key[0])
                } else {
                    unreachable!()
                };

                if let box Packet::ReplayPiece { ref mut nshards, .. } = m {
                    *nshards = 1;
                }

                let tx = &mut self.txs[shard];
                m.link_mut().src = index;
                m.link_mut().dst = tx.0;
                if tx.1.send(m).is_err() {
                    // we must be shutting down...
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
            context: payload::ReplayPieceContext::Regular { last: true }, ..
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
        if let Packet::ReplayPiece { ref mut nshards, .. } = *m {
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
