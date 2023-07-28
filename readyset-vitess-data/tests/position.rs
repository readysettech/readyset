use readyset_vitess_data::VStreamPosition;
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
