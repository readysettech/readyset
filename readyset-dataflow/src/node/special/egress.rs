use std::collections::HashMap;

use readyset_client::metrics::recorded;
use readyset_errors::{internal_err, invariant, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::node::special::packet_filter::PacketFilter;
use crate::payload::{ReplayPieceContext, SenderReplication};
use crate::prelude::*;

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Copy)]
struct EgressTxKey {
    node: NodeIndex,
    local: LocalNodeIndex,
    domain_index: DomainIndex,
    shard: usize,
}

#[derive(Serialize, Deserialize)]
pub struct EgressTx {
    node: NodeIndex,
    local: LocalNodeIndex,
    domain_index: DomainIndex,
    shard: usize,
    replication: SenderReplication,

    #[serde(skip)]
    sent_ctr: Option<metrics::Counter>,
    #[serde(skip)]
    dropped_ctr: Option<metrics::Counter>,
}

impl EgressTx {
    pub fn new(
        node: NodeIndex,
        local: LocalNodeIndex,
        domain_index: DomainIndex,
        shard: usize,
        replication: SenderReplication,
    ) -> Self {
        Self {
            node,
            local,
            domain_index,
            shard,
            replication,
            sent_ctr: None,
            dropped_ctr: None,
        }
    }

    fn key(&self) -> EgressTxKey {
        EgressTxKey {
            node: self.node,
            local: self.local,
            domain_index: self.domain_index,
            shard: self.shard,
        }
    }

    fn inc_sent(&mut self) {
        if let Some(ctr) = &self.sent_ctr {
            ctr.increment(1);
        } else {
            let node = self.node.index().to_string();
            let ctr =
                metrics::register_counter!(recorded::EGRESS_NODE_SENT_PACKETS, "node" => node);
            ctr.increment(1);
            self.sent_ctr.replace(ctr);
        }
    }

    fn inc_dropped(&mut self) {
        if let Some(ctr) = &self.dropped_ctr {
            ctr.increment(1);
        } else {
            let node = self.node.index().to_string();
            let ctr =
                metrics::register_counter!(recorded::EGRESS_NODE_DROPPED_PACKETS, "node" => node);
            ctr.increment(1);
            self.dropped_ctr.replace(ctr);
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct Egress {
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    txs: HashMap<EgressTxKey, EgressTx>,
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    tags: HashMap<Tag, NodeIndex>,
    packet_filter: PacketFilter,
}

impl Clone for Egress {
    fn clone(&self) -> Self {
        assert!(self.txs.is_empty());

        Self {
            txs: Default::default(),
            tags: self.tags.clone(),
            packet_filter: self.packet_filter.clone(),
        }
    }
}

impl Egress {
    pub fn add_tx(&mut self, tx: EgressTx) {
        self.txs.insert(tx.key(), tx);
    }

    pub fn add_for_filtering(&mut self, target: NodeIndex) {
        self.packet_filter.add_for_filtering(target);
    }

    pub fn add_tag(&mut self, tag: Tag, dst: NodeIndex) {
        self.tags.insert(tag, dst);
    }

    pub fn process(
        &mut self,
        message: &mut Option<Box<Packet>>,
        keyed_by: Option<&[usize]>,
        shard: usize,
        replica: usize,
        output: &mut dyn Executor,
    ) -> ReadySetResult<()> {
        let Self {
            txs,
            tags,
            packet_filter,
        } = self;

        // send any queued updates to all external children
        invariant!(!txs.is_empty());
        let txn = txs.len() - 1;

        // we need to find the ingress node following this egress according to the path
        // with replay.tag, and then forward this message only on the channel corresponding
        // to that ingress node.
        let replay_to = message
            .as_ref()
            .unwrap()
            .tag()
            .map(|tag| {
                tags.get(&tag).cloned().ok_or_else(|| {
                    internal_err!("egress node told about replay message, but not on replay path")
                })
            })
            .transpose()?;

        for (txi, ref mut tx) in txs.values_mut().enumerate() {
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
                message.take().unwrap()
            } else {
                // we know this is a data (not a replay)
                // because, a replay will force a take
                message.as_ref().map(|m| Box::new(m.clone_data())).unwrap()
            };

            // src is usually ignored and overwritten by ingress
            // *except* if the ingress is marked as a shard merger
            // in which case it wants to know about the shard
            m.link_mut().src = LocalNodeIndex::make(shard as u32);
            m.link_mut().dst = tx.local;

            // Take the packet through the filter. The filter will make any necessary modifications
            // to the packet to be sent, and tell us if we should send the packet or drop it.
            if !packet_filter.process(m.as_mut(), keyed_by, tx.node)? {
                tx.inc_dropped();
                continue;
            }
            tx.inc_sent();

            match tx.replication {
                SenderReplication::Same => output.send(
                    ReplicaAddress {
                        domain_index: tx.domain_index,
                        shard: tx.shard,
                        replica,
                    },
                    m,
                ),
                SenderReplication::Fanout { num_replicas } => {
                    if let Some(ReplayPieceContext::Partial {
                        requesting_replica, ..
                    }) = m.replay_piece_context()
                    {
                        // If the message is a piece of a replay that was requested by a particular
                        // replica, only replay to that replica
                        invariant!(
                            *requesting_replica < num_replicas,
                            "Replica index for replay piece context out-of-bounds"
                        );
                        output.send(
                            ReplicaAddress {
                                domain_index: tx.domain_index,
                                shard: tx.shard,
                                replica: *requesting_replica,
                            },
                            m.clone(),
                        )
                    } else {
                        // Otherwise, replay to all replicas
                        for replica in 0..num_replicas {
                            output.send(
                                ReplicaAddress {
                                    domain_index: tx.domain_index,
                                    shard: tx.shard,
                                    replica,
                                },
                                m.clone(),
                            )
                        }
                    }
                }
            }
            if take {
                break;
            }
        }
        Ok(())
    }
}
