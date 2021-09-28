use noria_client::backend::BackendBuilder;
use noria_client::test_helpers::{self, sleep};
use serial_test::serial;

mod common;
use common::PostgreSQLAdapter;
use postgres::NoTls;

fn setup() -> postgres::Config {
    test_helpers::setup::<PostgreSQLAdapter>(
        BackendBuilder::new().require_authentication(false),
        true,
        true,
    )
}

#[test]
#[serial]
fn create_table() {
    let config = setup();
    let mut client = config.connect(NoTls).unwrap();

    // NOTE: Currently, a race condition with noria startup means we have to wait until the
    // snapshot, which happens in the background of the noria server process, is finished before we
    // can start issuing any DDL queries to an adapter. Once that's fixed this sleep can go away.
    sleep();

    client
        .simple_query("CREATE TABLE cats (id int, PRIMARY KEY(id))")
        .unwrap();
    sleep();

    client
        .simple_query("INSERT INTO cats (id) VALUES (1)")
        .unwrap();
    sleep();

    let result = client
        .query_one("SELECT cats.id FROM cats WHERE cats.id = 1", &[])
        .unwrap()
        .get::<_, i32>(0);
    assert_eq!(result, 1)
}

#[test]
#[serial]
#[ignore] // needs proper detection of reads vs writes through fallback
fn prepare_execute_fallback() {
    let config = setup();
    let mut client = config.connect(NoTls).unwrap();

    sleep();

    client
        .simple_query("CREATE TABLE cats (id int, PRIMARY KEY(id))")
        .unwrap();

    client
        .execute("INSERT INTO cats (id) VALUES (1)", &[])
        .unwrap();
    sleep();

    // params in subqueries will likely always go to fallback
    let res = client
        .query(
            "SELECT id FROM (SELECT id FROM cats WHERE id = $1) sq",
            &[&1i32],
        )
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get::<_, i32>(0), 1);
}
