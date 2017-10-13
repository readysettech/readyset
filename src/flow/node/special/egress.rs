use std::collections::HashMap;
use flow::prelude::*;
use channel::{STcpSender, TcpSender};

/// Holds a transmit handle for an egress node. This struct appears Serializeable, but because it
/// contains a STcpSender, it will actually panic if serialized.
#[derive(Serialize, Deserialize)]
struct EgressTx {
    node: NodeIndex,
    local: LocalNodeIndex,
    sender: STcpSender<Box<Packet>>,
    is_local: bool,
}

#[derive(Serialize, Deserialize)]
pub struct Egress {
    txs: Vec<EgressTx>,
    tags: HashMap<Tag, NodeIndex>,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Self {
            txs: Vec::new(),
            tags: self.tags.clone(),
        }
    }
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
    pub fn add_tx(
        &mut self,
        dst_g: NodeIndex,
        dst_l: LocalNodeIndex,
        tx: TcpSender<Box<Packet>>,
        is_local: bool,
    ) {
        self.txs.push(EgressTx {
            node: dst_g,
            local: dst_l,
            sender: STcpSender(tx),
            is_local,
        });

        // Sort txs so that the local connections come last. This simplifies the logic in process.
        self.txs.sort_by_key(|tx| tx.is_local);
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);
    }

    pub fn process(&mut self, m: &mut Option<Box<Packet>>, shard: usize) {
        let &mut Self {
            ref mut txs,
            ref tags,
        } = self;

        // send any queued updates to all external children
        assert!(txs.len() > 0);
        let txn = txs.len() - 1;

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = m.as_ref().unwrap().tag().map(|tag| {
            tags.get(&tag)
                .map(|n| *n)
                .expect("egress node told about replay message, but not on replay path")
        });

        for (txi, ref mut tx) in txs.iter_mut().enumerate() {
            if !tx.is_local {
                let m = m.as_mut().unwrap();
                m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
                m.link_mut().dst = tx.local;
                if tx.sender.send_ref(m).is_err() {
                    break;
                }
            } else {
                let mut take = txi == txn;
                if let Some(replay_to) = replay_to.as_ref() {
                    if *replay_to == tx.node {
                        take = true;
                    } else {
                        continue;
                    }
                }

                // Avoid cloning if this is last send
                let mut m = if take {
                    m.take().unwrap()
                } else {
                    // we know this is a data (not a replay)
                    // because, a replay will force a take
                    m.as_ref().map(|m| box m.clone_data()).unwrap()
                };

                // src is usually ignored and overwritten by ingress
                // *except* if the ingress is marked as a shard merger
                // in which case it wants to know about the shard
                m.link_mut().src = unsafe { LocalNodeIndex::make(shard as u32) };
                m.link_mut().dst = tx.local;

                if tx.sender.send(m.make_local()).is_err() {
                    // we must be shutting down...
                    break;
                }

                if take {
                    break;
                }
            }
        }
    }
}
