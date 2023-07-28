use std::fmt::Display;

use vitess_grpc::binlogdata::VGtid;

#[derive(Debug, Clone)]
pub struct ShardPosition {
    keyspace: String,
    shard: String,
    gtid: String,
}

#[derive(Debug, Clone)]
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
