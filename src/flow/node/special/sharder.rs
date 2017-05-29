use std::sync::mpsc;
use flow::prelude::*;
use flow::payload;
use vec_map::VecMap;
use petgraph::graph::NodeIndex;

pub struct Sharder {
    txs: Vec<(NodeAddress, mpsc::SyncSender<Box<Packet>>)>,
    sharded: VecMap<Box<Packet>>,
    shard_by: usize,
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

    pub fn add_shard(&mut self, dst: NodeAddress, tx: mpsc::SyncSender<Box<Packet>>) {
        self.txs.push((dst, tx));
    }

    #[inline]
    fn to_shard(&self, r: &Record) -> usize {
        self.shard(&r[self.shard_by])
    }

    #[inline]
    fn shard(&self, dt: &DataType) -> usize {
        match *dt {
            DataType::Int(n) => n as usize % self.txs.len(),
            DataType::BigInt(n) => n as usize % self.txs.len(),
            _ => unimplemented!(),
        }
    }

    pub fn process(&mut self, m: &mut Option<Box<Packet>>, index: NodeIndex) {
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
                            link: Link::new(index.into(), dst),
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
                   } = m {
                // we only need to send this to the shard responsible for the key being replayed!
                // ugh, I'm sad about this double destruct, but it's necessary for borrowing :(
                let shard = if let box Packet::ReplayPiece {
                           context: payload::ReplayPieceContext::Partial { ref for_key, .. }, ..
                       } = m {
                    assert_eq!(for_key.len(), 1);
                    self.shard(&for_key[0])
                } else {
                    unreachable!()
                };

                let tx = &mut self.txs[shard];
                m.link_mut().src = index.into();
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

        if let Packet::ReplayPiece {
                   context: payload::ReplayPieceContext::Regular { last: true }, ..
               } = *m {
            // this is the last replay piece for a full replay
            // we need to make sure it gets to every shard so they know to ready the node
            for shard in 0..self.txs.len() {
                self.sharded
                    .entry(shard)
                    .or_insert_with(|| box m.clone_data());
            }
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
