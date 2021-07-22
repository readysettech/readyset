use crate::*;
use mysql::prelude::Queryable;
use mysql::Value;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn create_table_insert_test() {
    let cluster_name = "ct_create_table_insert";
    let mut deployment = DeploymentParams::new(
        cluster_name,
        NoriaBinarySource::Build(BuildParams {
            root_project_path: get_project_root(),
            target_dir: get_project_root().join("test_target"),
            release: true,
            rebuild: false,
        }),
    );
    deployment.add_server(ServerParams::default());
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql_adapter();

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut conn = mysql::Conn::new(opts.clone()).unwrap();
    let _ = conn
        .query_drop(
            r"CREATE TABLE t1 (
        uid INT NOT NULL,
        value INT NOT NULL,
    );",
        )
        .unwrap();
    conn.query_drop(r"INSERT INTO t1 VALUES (1, 4);").unwrap();

    let res: Vec<(i32, i32)> = conn.query(r"SELECT * FROM t1;").unwrap();
    assert_eq!(res, vec![(1, 4)]);

    deployment.teardown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn show_tables_test() {
    let cluster_name = "ct_show_tables";
    let mut deployment = DeploymentParams::new(
        cluster_name,
        NoriaBinarySource::Build(BuildParams {
            root_project_path: get_project_root(),
            target_dir: get_project_root().join("test_target"),
            release: true,
            rebuild: false,
        }),
    );
    deployment.add_server(ServerParams::default());
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut conn = mysql::Conn::new(opts.clone()).unwrap();
    let _ = conn
        .query_drop(r"CREATE TABLE t2a (uid INT NOT NULL, value INT NOT NULL,);")
        .unwrap();
    let _ = conn
        .query_drop(r"CREATE TABLE t2b (uid INT NOT NULL, value INT NOT NULL,);")
        .unwrap();

    let tables: Vec<String> = conn.query("SHOW TABLES;").unwrap();
    deployment.teardown().await.unwrap();
    assert_eq!(tables, vec!["t2a", "t2b"]);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn describe_table_test() {
    let cluster_name = "ct_describe_table";
    let mut deployment = DeploymentParams::new(
        cluster_name,
        NoriaBinarySource::Build(BuildParams {
            root_project_path: get_project_root(),
            target_dir: get_project_root().join("test_target"),
            release: true,
            rebuild: false,
        }),
    );
    deployment.add_server(ServerParams::default());
    deployment.add_server(ServerParams::default());
    deployment.deploy_mysql();

    let mut deployment = start_multi_process(deployment).await.unwrap();
    let opts = mysql::Opts::from_url(&deployment.mysql_connection_str().unwrap()).unwrap();
    let mut conn = mysql::Conn::new(opts.clone()).unwrap();
    let _ = conn
        .query_drop(r"CREATE TABLE t3 (uid INT NOT NULL, value INT NOT NULL,);")
        .unwrap();

    let table: Vec<mysql::Row> = conn.query("DESCRIBE t3;").unwrap();
    let descriptor = table.get(0).unwrap();
    let cols = descriptor.columns_ref();
    let cols = cols
        .iter()
        .map(|c| c.name_ref())
        .into_iter()
        .collect::<Vec<_>>();
    let vals: Vec<Value> = descriptor.clone().unwrap();

    let cols_truth = vec![
        "Field".as_bytes(),
        "Type".as_bytes(),
        "Null".as_bytes(),
        "Key".as_bytes(),
        "Default".as_bytes(),
        "Extra".as_bytes(),
    ];
    let vals_truth = vec![
        Value::Bytes("uid".as_bytes().to_vec()),
        Value::Bytes("int".as_bytes().to_vec()),
        Value::Bytes("NO".as_bytes().to_vec()),
        Value::Bytes("".as_bytes().to_vec()),
        Value::NULL,
        Value::Bytes("".as_bytes().to_vec()),
    ];

    deployment.teardown().await.unwrap();

    assert_eq!(vals, vals_truth);
    assert_eq!(cols, cols_truth);
}
