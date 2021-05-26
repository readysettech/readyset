use crate::*;
use mysql::prelude::Queryable;
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
