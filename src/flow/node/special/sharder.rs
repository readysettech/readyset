use std::sync::mpsc;
use flow::prelude::*;
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
        match r[self.shard_by] {
            DataType::Int(n) => n as usize % self.txs.len(),
            DataType::BigInt(n) => n as usize % self.txs.len(),
            _ => unimplemented!(),
        }
    }

    pub fn process(&mut self, m: &mut Option<Box<Packet>>, index: NodeIndex) {
        // we need to shard the records inside `m` by their key,
        let mut m = m.take().unwrap();
        for record in m.take_data() {
            let shard = self.to_shard(&record);
            let p = self.sharded
                .entry(shard)
                .or_insert_with(|| box m.clone_data());
            p.map_data(|rs| rs.push(record));
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
