use replication_offset::vitess::VStreamPosition;
use vitess_grpc::binlogdata::{ShardGtid, VGtid};

#[test]
fn current() {
    let position = VStreamPosition::current_for_keyspace("test");
    assert_eq!(position.to_string(), "VitessPosition[test::current]");
}

#[test]
fn from_vgtid() {
    let grpc_position = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857".to_string(),
            ..Default::default()
        }],
    };

    let position: VStreamPosition = grpc_position.into();
    assert_eq!(
        position.to_string(),
        "VitessPosition[test:0:MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857]"
    );
}

#[test]
fn to_grpc() {
    let grpc_pos = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857".to_string(),
            ..Default::default()
        }],
    };

    let pos: VStreamPosition = grpc_pos.clone().into();
    let new_grpc_pos: VGtid = pos.into();

    assert_eq!(grpc_pos, new_grpc_pos);
}

#[test]
fn partial_ord_eq() {
    let grpc_position = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857".to_string(),
            ..Default::default()
        }],
    };

    let position1: VStreamPosition = grpc_position.into();
    let position2: VStreamPosition = position1.clone();

    let res = position1.partial_cmp(&position2);
    assert!(res.is_some());
    assert_eq!(res.unwrap(), std::cmp::Ordering::Equal);
}

#[test]
fn partial_ord_different_len() {
    let grpc_position1 = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857".to_string(),
            ..Default::default()
        }],
    };

    let grpc_position2 = VGtid {
        shard_gtids: vec![
            grpc_position1.shard_gtids[0].clone(),
            ShardGtid {
                keyspace: "test".to_string(),
                shard: "1".to_string(),
                gtid: "MySQL56/a89827aa-db0b-11ed-b39e-99acb91bd99:1-25".to_string(),
                ..Default::default()
            },
        ],
    };

    let position1: VStreamPosition = grpc_position1.into();
    let position2: VStreamPosition = grpc_position2.into();

    assert!(position1.partial_cmp(&position2).is_none());
}

#[test]
fn partial_ord_different_shard() {
    let grpc_position1 = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857".to_string(),
            ..Default::default()
        }],
    };

    let grpc_position2 = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "1".to_string(),
            gtid: "MySQL56/a89827aa-db0b-11ed-b39e-99acb91bd99:1-25".to_string(),
            ..Default::default()
        }],
    };

    let position1: VStreamPosition = grpc_position1.into();
    let position2: VStreamPosition = grpc_position2.into();

    assert!(position1.partial_cmp(&position2).is_none());
}

#[test]
fn partial_ord_different_gtid() {
    let grpc_position1 = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857".to_string(),
            ..Default::default()
        }],
    };

    let grpc_position2 = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/a89827aa-db0b-11ed-b39e-99acb91bd99:1-25".to_string(),
            ..Default::default()
        }],
    };

    let position1: VStreamPosition = grpc_position1.into();
    let position2: VStreamPosition = grpc_position2.into();

    assert!(position1.partial_cmp(&position2).is_none());
}

#[test]
fn partial_ord_less_or_more() {
    let grpc_position1 = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-857".to_string(),
            ..Default::default()
        }],
    };

    let grpc_position2 = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: "test".to_string(),
            shard: "0".to_string(),
            gtid: "MySQL56/e89827ae-de0a-11ed-b39e-82acb91bd404:1-858".to_string(),
            ..Default::default()
        }],
    };

    let position1: VStreamPosition = grpc_position1.into();
    let position2: VStreamPosition = grpc_position2.into();

    assert_eq!(
        position1.partial_cmp(&position2).unwrap(),
        std::cmp::Ordering::Less
    );

    assert_eq!(
        position2.partial_cmp(&position1).unwrap(),
        std::cmp::Ordering::Greater
    );
}
