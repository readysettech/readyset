use std::fmt::Display;

use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use readyset_vitess_data::GtidSet;
use serde::{Deserialize, Serialize};
use vitess_grpc::binlogdata::VGtid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPosition {
    keyspace: String,
    shard: String,
    gtid: GtidSet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VStreamPosition {
    shard_positions: Vec<ShardPosition>,
}

impl VStreamPosition {
    pub fn current_for_keyspace(keyspace: &str) -> Self {
        Self {
            shard_positions: vec![ShardPosition {
                keyspace: keyspace.to_string(),
                shard: "".to_string(),
                gtid: GtidSet::Current,
            }],
        }
    }
}

impl PartialOrd for VStreamPosition {
    fn partial_cmp(&self, other_pos: &Self) -> Option<std::cmp::Ordering> {
        // Cannot compare positions with different number of shards/keyspaces
        if self.shard_positions.len() != other_pos.shard_positions.len() {
            return None;
        }

        // Check all gtids while making sure that the keyspaces and shards match
        for (self_shard_pos, other_shard_pos) in self
            .shard_positions
            .iter()
            .zip(other_pos.shard_positions.iter())
        {
            if self_shard_pos.keyspace != other_shard_pos.keyspace
                || self_shard_pos.shard != other_shard_pos.shard
            {
                return None;
            }

            let res = self_shard_pos.gtid.partial_cmp(&other_shard_pos.gtid);
            if res != Some(std::cmp::Ordering::Equal) {
                return res;
            }
        }

        Some(std::cmp::Ordering::Equal)
    }
}

impl TryFrom<&VGtid> for VStreamPosition {
    type Error = ReadySetError;

    fn try_from(vgtid: &VGtid) -> ReadySetResult<Self> {
        let shard_positions = vgtid
            .shard_gtids
            .iter()
            .map(|shard_gtid| {
                let gtid_string: &str = shard_gtid.gtid.as_ref();
                let gtid_set = gtid_string
                    .try_into()
                    .map_err(|_| internal_err!("invalid VGTID string: {}", gtid_string))?;

                Ok(ShardPosition {
                    keyspace: shard_gtid.keyspace.clone(),
                    shard: shard_gtid.shard.clone(),
                    gtid: gtid_set,
                })
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        Ok(Self { shard_positions })
    }
}

impl TryFrom<VGtid> for VStreamPosition {
    type Error = ReadySetError;
    fn try_from(vgtid: VGtid) -> ReadySetResult<Self> {
        (&vgtid).try_into()
    }
}

impl From<VStreamPosition> for VGtid {
    fn from(position: VStreamPosition) -> Self {
        let shard_gtids = position
            .shard_positions
            .into_iter()
            .map(|shard_position| vitess_grpc::binlogdata::ShardGtid {
                keyspace: shard_position.keyspace,
                shard: shard_position.shard,
                gtid: shard_position.gtid.to_string(),
                ..Default::default()
            })
            .collect();

        Self { shard_gtids }
    }
}

impl From<&VStreamPosition> for VGtid {
    fn from(position: &VStreamPosition) -> Self {
        position.clone().into()
    }
}

impl Display for ShardPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.keyspace, self.shard, self.gtid)
    }
}

impl Display for VStreamPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut shard_positions = self.shard_positions.iter();

        write!(f, "VitessPosition[")?;
        if let Some(shard_position) = shard_positions.next() {
            write!(f, "{}", shard_position)?;
        }

        for shard_position in shard_positions {
            write!(f, ",{}", shard_position)?;
        }
        write!(f, "]")?;

        Ok(())
    }
}
