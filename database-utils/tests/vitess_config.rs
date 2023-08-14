use database_utils::VitessConfig;

#[test]
fn test_vitess_config() {
    let url = "vitess://root:@localhost:15301/commerce?mysql_port=15302";
    let config = VitessConfig::from_str(url).expect("failed to parse vitess url");
    assert_eq!(config.host, "localhost");
    assert_eq!(config.grpc_port, 15301);
    assert_eq!(config.mysql_port, 15302);
    assert_eq!(config.username, "root");
    assert_eq!(config.password, "");
    assert_eq!(config.keyspace, "commerce");
    assert_eq!(config.shard, "0");
}
