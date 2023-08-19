use database_utils::VitessConfig;

#[test]
fn test_vitess_config_parsing() {
    let url = "vitess://root:secret@foo.bar:10000/commerce?mysql_port=20000&shard=2";
    let config = VitessConfig::from_str(url).expect("failed to parse vitess url");
    assert_eq!(config.host, "foo.bar");
    assert_eq!(config.grpc_port, 10000);
    assert_eq!(config.mysql_port, 20000);
    assert_eq!(config.username, "root");
    assert_eq!(config.password, "secret");
    assert_eq!(config.keyspace, "commerce");
    assert_eq!(config.shard, "2");
}

#[test]
fn test_vitess_config_defaults() {
    let url = "vitess://root@localhost/commerce";
    let config = VitessConfig::from_str(url).expect("failed to parse vitess url");
    assert_eq!(config.host, "localhost");
    assert_eq!(config.grpc_port, 15301);
    assert_eq!(config.mysql_port, 15302);
    assert_eq!(config.username, "root");
    assert_eq!(config.password, "");
    assert_eq!(config.keyspace, "commerce");
    assert_eq!(config.shard, "0");
}

#[test]
fn test_vitess_config_invalid() {
    let url = "vitess://root@localhost/commerce?mysql_port=invalid";
    let config = VitessConfig::from_str(url);
    assert!(config.is_err());
}
