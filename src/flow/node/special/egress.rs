use std::sync::mpsc;
use std::collections::HashMap;
use flow::prelude::*;
use petgraph::graph::NodeIndex;

pub struct Egress {
    txs: Vec<(NodeAddress, NodeAddress, mpsc::SyncSender<Box<Packet>>)>,
    tags: HashMap<Tag, NodeAddress>,
}

impl Default for Egress {
    fn default() -> Self {
        Self {
            tags: Default::default(),
            txs: Default::default(),
        }
    }
}

impl Egress {
    pub fn add_tx(&mut self,
                  dst_g: NodeAddress,
                  dst_l: NodeAddress,
                  tx: mpsc::SyncSender<Box<Packet>>) {
        self.txs.push((dst_g, dst_l, tx));
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeAddress) {
        self.tags.insert(tag, dst);
    }

    pub fn process(&mut self, m: &mut Option<Box<Packet>>, index: NodeIndex) {
        let &mut Self {
                     ref mut txs,
                     ref tags,
                 } = self;

        // send any queued updates to all external children
        let txn = txs.len() - 1;

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = m.as_ref()
            .unwrap()
            .tag()
            .map(|tag| {
                     tags.get(&tag)
                         .map(|n| *n)
                         .expect("egress node told about replay message, but not on replay path")
                 });

        for (txi, &mut (ref globaddr, dst, ref mut tx)) in txs.iter_mut().enumerate() {
            let mut take = txi == txn;
            if let Some(replay_to) = replay_to.as_ref() {
                if replay_to == globaddr {
                    take = true;
                } else {
                    continue;
                }
            }

            // avoid cloning if this is last send
            let mut m = if take {
                m.take().unwrap()
            } else {
                // we know this is a data (not a replay)
                // because, a replay will force a take
                m.as_ref().map(|m| box m.clone_data()).unwrap()
            };

            m.link_mut().src = index.into();
            m.link_mut().dst = dst;

            if tx.send(m).is_err() {
                // we must be shutting down...
                break;
            }

            if take {
                break;
            }
        }
    }
}
