use std::fmt::Display;

use serde::{Deserialize, Serialize};
use vitess_grpc::binlogdata::VGtid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPosition {
    keyspace: String,
    shard: String,
    gtid: String,
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
                gtid: "current".to_string(),
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

            let res = vgtid_partial_cmp(&self_shard_pos.gtid, &other_shard_pos.gtid);
            if res != Some(std::cmp::Ordering::Equal) {
                return res;
            }
        }

        Some(std::cmp::Ordering::Equal)
    }
}

// Compare two VGTID values from vitess for ordering purposes.
// FIXME: This is a hacky implementation that works for vttestserver, but need to verify it against
// the actual vitess implementation.
fn vgtid_partial_cmp(vgtid1: &str, vgtid2: &str) -> Option<std::cmp::Ordering> {
    if vgtid1 == vgtid2 {
        return Some(std::cmp::Ordering::Equal);
    }

    // Format: MySQL56/b6f64869-4457-11ee-a149-4a61a0626815:1-35
    // Fields: <prefix>/<uuid>:<start>-<end>
    let gtid1_parts: Vec<&str> = vgtid1.split('/').collect();
    let gtid2_parts: Vec<&str> = vgtid2.split('/').collect();

    if gtid1_parts.len() != 2 || gtid2_parts.len() != 2 {
        return None;
    }

    if gtid1_parts[0] != gtid2_parts[0] {
        return None;
    }

    let gtid1_uuid_parts: Vec<&str> = gtid1_parts[1].split(':').collect();
    let gtid2_uuid_parts: Vec<&str> = gtid2_parts[1].split(':').collect();

    if gtid1_uuid_parts.len() != 2 || gtid2_uuid_parts.len() != 2 {
        return None;
    }

    if gtid1_uuid_parts[0] != gtid2_uuid_parts[0] {
        return None;
    }

    let gtid1_start_end_parts: Vec<&str> = gtid1_uuid_parts[1].split('-').collect();
    let gtid2_start_end_parts: Vec<&str> = gtid2_uuid_parts[1].split('-').collect();

    if gtid1_start_end_parts.len() != 2 || gtid2_start_end_parts.len() != 2 {
        return None;
    }

    let gtid1_start: u64 = gtid1_start_end_parts[0].parse().ok()?;
    let gtid1_end: u64 = gtid1_start_end_parts[1].parse().ok()?;
    let gtid2_start: u64 = gtid2_start_end_parts[0].parse().ok()?;
    let gtid2_end: u64 = gtid2_start_end_parts[1].parse().ok()?;
    if gtid1_start != gtid2_start {
        return None;
    }

    if gtid1_end < gtid2_end {
        return Some(std::cmp::Ordering::Less);
    } else if gtid1_end > gtid2_end {
        return Some(std::cmp::Ordering::Greater);
    }

    None
}

impl From<&VGtid> for VStreamPosition {
    fn from(vgtid: &VGtid) -> Self {
        let shard_positions = vgtid
            .shard_gtids
            .iter()
            .map(|shard_gtid| ShardPosition {
                keyspace: shard_gtid.keyspace.clone(),
                shard: shard_gtid.shard.clone(),
                gtid: shard_gtid.gtid.clone(),
            })
            .collect();

        Self { shard_positions }
    }
}

impl From<VGtid> for VStreamPosition {
    fn from(vgtid: VGtid) -> Self {
        (&vgtid).into()
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
                gtid: shard_position.gtid,
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
