use mysql_async::prelude::Queryable;
use mysql_time::MysqlTime;
use noria::DataType as D;
use noria::{
    consensus::{Authority, LocalAuthority, LocalAuthorityStore},
    ControllerHandle, DataType, ReadySetResult, TinyText,
};
use noria_server::Builder;
use replicators::NoriaAdapter;
use std::env;
use std::sync::Arc;

// Postgres does not accept MySQL escapes, so rename the table before the query
const PGSQL_RENAME: (&str, &str) = ("`groups`", "groups");

const CREATE_SCHEMA: &str = "
    DROP TABLE IF EXISTS `groups` CASCADE;
    DROP VIEW IF EXISTS noria_view;
    CREATE TABLE `groups` (
        id int NOT NULL PRIMARY KEY,
        string varchar(20) NOT NULL,
        bignum int
    );
    CREATE VIEW noria_view AS SELECT id,string,bignum FROM `groups` ORDER BY id ASC";

const POPULATE_SCHEMA: &str =
    "INSERT INTO `groups` VALUES (1, 'abc', 2), (2, 'bcd', 3), (40, 'xyz', 4)";

/// A convinience init to convert 3 character byte slice to TinyText noria type
const fn tiny<const N: usize>(text: &[u8; N]) -> DataType {
    DataType::TinyText(TinyText::from_arr(text))
}

const SNAPSHOT_RESULT: &[&[DataType]] = &[
    &[D::Int(1), tiny(b"abc"), D::Int(2)],
    &[D::Int(2), tiny(b"bcd"), D::Int(3)],
    &[D::Int(40), tiny(b"xyz"), D::Int(4)],
];

const TESTS: &[(&str, &str, &[&[DataType]])] = &[
    (
        "Test UPDATE key column replication",
        "UPDATE `groups` SET id=id+10",
        &[
            &[D::Int(11), tiny(b"abc"), D::Int(2)],
            &[D::Int(12), tiny(b"bcd"), D::Int(3)],
            &[D::Int(50), tiny(b"xyz"), D::Int(4)],
        ],
    ),
    (
        "Test DELETE replication",
        "DELETE FROM `groups` WHERE string='bcd'",
        &[
            &[D::Int(11), tiny(b"abc"), D::Int(2)],
            &[D::Int(50), tiny(b"xyz"), D::Int(4)],
        ],
    ),
    (
        "Test INSERT replication",
        "INSERT INTO `groups` VALUES (1, 'abc', 2), (2, 'bcd', 3), (40, 'xyz', 4)",
        &[
            &[D::Int(1), tiny(b"abc"), D::Int(2)],
            &[D::Int(2), tiny(b"bcd"), D::Int(3)],
            &[D::Int(11), tiny(b"abc"), D::Int(2)],
            &[D::Int(40), tiny(b"xyz"), D::Int(4)],
            &[D::Int(50), tiny(b"xyz"), D::Int(4)],
        ],
    ),
    (
        "Test UPDATE non-key column replication",
        "UPDATE `groups` SET bignum=id+10",
        &[
            &[D::Int(1), tiny(b"abc"), D::Int(11)],
            &[D::Int(2), tiny(b"bcd"), D::Int(12)],
            &[D::Int(11), tiny(b"abc"), D::Int(21)],
            &[D::Int(40), tiny(b"xyz"), D::Int(50)],
            &[D::Int(50), tiny(b"xyz"), D::Int(60)],
        ],
    ),
];

/// Test query we issue after replicator disconnect
const DISCONNECT_QUERY: &str = "INSERT INTO `groups` VALUES (3, 'abc', 2), (5, 'xyz', 4)";
/// Test result after replicator reconnects and catches up
const RECONNECT_RESULT: &[&[DataType]] = &[
    &[D::Int(1), tiny(b"abc"), D::Int(11)],
    &[D::Int(2), tiny(b"bcd"), D::Int(12)],
    &[D::Int(3), tiny(b"abc"), D::Int(2)],
    &[D::Int(5), tiny(b"xyz"), D::Int(4)],
    &[D::Int(11), tiny(b"abc"), D::Int(21)],
    &[D::Int(40), tiny(b"xyz"), D::Int(50)],
    &[D::Int(50), tiny(b"xyz"), D::Int(60)],
];

struct TestHandle {
    url: String,
    noria: noria_server::Handle,
    authority: Arc<Authority>,
    // We spin a whole runtime for the replication task because the tokio postgres
    // connection spawns a background task we can only terminate by dropping the runtime
    replication_rt: Option<tokio::runtime::Runtime>,
    ready_notify: Option<Arc<tokio::sync::Notify>>,
}

impl Drop for TestHandle {
    fn drop(&mut self) {
        if let Some(rt) = self.replication_rt.take() {
            rt.shutdown_background();
        }
    }
}

enum DbConnection {
    MySQL(mysql_async::Conn),
    PostgreSQL(
        tokio_postgres::Client,
        tokio::task::JoinHandle<ReadySetResult<()>>,
    ),
}

impl DbConnection {
    async fn connect(url: &str) -> ReadySetResult<Self> {
        if url.starts_with("mysql") {
            let opts: mysql_async::Opts = url.parse().unwrap();
            let client = mysql_async::Conn::new(opts).await?;
            Ok(DbConnection::MySQL(client))
        } else if url.starts_with("postgresql") {
            let (client, conn) = tokio_postgres::connect(&pgsql_url(), tokio_postgres::NoTls)
                .await
                .unwrap();
            let connection_handle = tokio::spawn(async move { conn.await.map_err(Into::into) });
            Ok(DbConnection::PostgreSQL(client, connection_handle))
        } else {
            unimplemented!()
        }
    }

    async fn query(&mut self, query: &str) -> ReadySetResult<()> {
        match self {
            DbConnection::MySQL(c) => {
                c.query_drop(query).await?;
            }
            DbConnection::PostgreSQL(c, _) => {
                let query = query.replace(PGSQL_RENAME.0, PGSQL_RENAME.1);
                c.simple_query(query.as_str()).await?;
            }
        }
        Ok(())
    }

    async fn stop(self) {
        match self {
            DbConnection::MySQL(_) => {}
            DbConnection::PostgreSQL(_, h) => {
                h.abort();
                let _ = h.await;
            }
        }
    }
}

impl TestHandle {
    async fn start_noria(url: String) -> ReadySetResult<TestHandle> {
        let authority_store = Arc::new(LocalAuthorityStore::new());
        let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
            authority_store,
        )));
        TestHandle::start_with_authority(url, authority).await
    }

    async fn start_with_authority(
        url: String,
        authority: Arc<Authority>,
    ) -> ReadySetResult<TestHandle> {
        readyset_logging::init_test_logging();
        let mut builder = Builder::for_tests();
        let mut persistence = noria_server::PersistenceParameters::default();
        persistence.mode = noria_server::DurabilityMode::DeleteOnExit;
        builder.set_persistence(persistence);
        let noria = builder.start(Arc::clone(&authority)).await.unwrap();

        let mut handle = TestHandle {
            url,
            noria,
            authority,
            replication_rt: None,
            ready_notify: Some(Default::default()),
        };

        handle.start_repl().await?;

        Ok(handle)
    }

    async fn controller(&self) -> ControllerHandle {
        ControllerHandle::new(Arc::clone(&self.authority)).await
    }

    async fn stop(mut self) {
        self.stop_repl().await;
        self.noria.shutdown();
        self.noria.wait_done().await;
    }

    async fn stop_repl(&mut self) {
        if let Some(rt) = self.replication_rt.take() {
            rt.shutdown_background();
        }
    }

    async fn start_repl(&mut self) -> ReadySetResult<()> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let controller = ControllerHandle::new(Arc::clone(&self.authority)).await;

        let _ = runtime.spawn(NoriaAdapter::start_with_url(
            self.url.clone(),
            controller,
            None,
            self.ready_notify.clone(),
        ));

        if let Some(rt) = self.replication_rt.replace(runtime) {
            rt.shutdown_background();
        }

        Ok(())
    }

    async fn check_results(
        &mut self,
        view_name: &str,
        test_name: &str,
        test_results: &[&[DataType]],
    ) -> ReadySetResult<()> {
        const MAX_ATTEMPTS: usize = 6;
        let mut attempt: usize = 0;
        loop {
            match self.check_results_inner(view_name).await {
                Err(_) if attempt < MAX_ATTEMPTS => {
                    // Sometimes things are slow in CI, so we retry a few times before giving up
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                }
                Ok(res) if res != test_results && attempt < MAX_ATTEMPTS => {
                    // Sometimes things are slow in CI, so we retry a few times before giving up
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                }
                Ok(res) => {
                    assert_eq!(res, *test_results, "{} incorrect", test_name);
                    break Ok(());
                }
                Err(err) => break Err(err),
            }
        }
    }

    async fn check_results_inner(&mut self, view_name: &str) -> ReadySetResult<Vec<Vec<DataType>>> {
        let mut getter = self.controller().await.view(view_name).await?;
        let results = getter.lookup(&[0.into()], true).await?;
        let mut results = results.as_ref().to_owned();
        results.sort(); // Simple `lookup` does not sort the results, so we just sort them ourselves
        Ok(results)
    }
}

async fn replication_test_inner(url: &str) -> ReadySetResult<()> {
    let mut client = DbConnection::connect(url).await?;
    client.query(CREATE_SCHEMA).await?;
    client.query(POPULATE_SCHEMA).await?;

    let mut ctx = TestHandle::start_noria(url.to_string()).await?;
    ctx.ready_notify.as_ref().unwrap().notified().await;

    ctx.check_results("noria_view", "Snapshot", SNAPSHOT_RESULT)
        .await?;

    for (test_name, test_query, test_results) in TESTS {
        client.query(test_query).await?;
        ctx.check_results("noria_view", test_name, *test_results)
            .await?;
    }

    // Stop the replication task, issue some queries then check they are picked up after reconnect
    ctx.stop_repl().await;
    client.query(DISCONNECT_QUERY).await?;

    // Make sure no replication takes place for real
    ctx.check_results("noria_view", "Disconnected", TESTS[TESTS.len() - 1].2)
        .await?;

    // Resume replication
    ctx.start_repl().await?;
    ctx.check_results("noria_view", "Reconnect", RECONNECT_RESULT)
        .await?;

    client.stop().await;
    ctx.stop().await;

    Ok(())
}

fn pgsql_url() -> String {
    format!(
        "postgresql://postgres:noria@{}:{}/noria",
        env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("PGPORT").unwrap_or_else(|_| "5432".into()),
    )
}

fn mysql_url() -> String {
    format!(
        "mysql://root:noria@{}:{}/noria",
        env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
    )
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn pgsql_replication() -> ReadySetResult<()> {
    replication_test_inner(&pgsql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn mysql_replication() -> ReadySetResult<()> {
    replication_test_inner(&mysql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
/// This test checks that when writes and replication happen in parallel
/// noria correctly catches up from binlog
/// NOTE: If this test flakes, please notify Vlad
async fn mysql_replication_catch_up() -> ReadySetResult<()> {
    const TOTAL_INSERTS: usize = 4000;

    let url = &mysql_url();
    let mut client = DbConnection::connect(url).await?;
    client
        .query(
            "
            DROP TABLE IF EXISTS `catch_up` CASCADE;
            DROP TABLE IF EXISTS `catch_up_pk` CASCADE;
            DROP VIEW IF EXISTS catch_up_view;
            DROP VIEW IF EXISTS catch_up_pk_view;
            CREATE TABLE `catch_up` (
                id int,
                val varchar(255)
            );
            CREATE TABLE `catch_up_pk` (
                id int PRIMARY KEY,
                val varchar(255)
            );
            CREATE VIEW catch_up_view AS SELECT * FROM `catch_up`;
            CREATE VIEW catch_up_pk_view AS SELECT * FROM `catch_up_pk`;",
        )
        .await?;

    // Begin the inserter task before we begin replication. It should have sufficient
    // inserts to perform to last past the replication process. We use a keyless table
    // to make sure we not only get all of the inserts, but also all of the inserts are
    // processed exactly once
    let inserter: tokio::task::JoinHandle<ReadySetResult<DbConnection>> =
        tokio::spawn(async move {
            for idx in 0..TOTAL_INSERTS {
                client
                    .query("INSERT INTO `catch_up` VALUES (100, 'I am a teapot')")
                    .await?;

                client
                    .query(&format!(
                        "INSERT INTO `catch_up_pk` VALUES ({}, 'I am a teapot')",
                        idx
                    ))
                    .await?;
            }

            Ok(client)
        });

    // Sleep just a bit to make sure we have some writes done
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let ctx = TestHandle::start_noria(url.to_string()).await?;
    ctx.ready_notify.as_ref().unwrap().notified().await;

    let mut client = inserter.await.unwrap()?;

    // Sleep just more to allow propagation time
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Finished inserting, check view result
    let mut getter = ctx.controller().await.view("catch_up_view").await?;
    let results = getter.lookup(&[0.into()], true).await?;
    assert_eq!(results.len(), TOTAL_INSERTS);

    let mut getter = ctx.controller().await.view("catch_up_pk_view").await?;
    let results = getter.lookup(&[0.into()], true).await?;
    assert_eq!(results.len(), TOTAL_INSERTS);

    ctx.stop().await;

    client
        .query(
            "
        DROP TABLE IF EXISTS `catch_up` CASCADE;
        DROP TABLE IF EXISTS `catch_up_pk` CASCADE;
        DROP VIEW IF EXISTS catch_up_view;
        DROP VIEW IF EXISTS catch_up_pk_view;",
        )
        .await?;

    client.stop().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn mysql_replication_many_tables() -> ReadySetResult<()> {
    const TOTAL_TABLES: usize = 300;

    let url = &mysql_url();
    let mut client = DbConnection::connect(url).await?;

    for t in 0..TOTAL_TABLES {
        let tbl_name = format!("t{}", t);
        client
            .query(&format!(
                "DROP TABLE IF EXISTS `{0}` CASCADE; CREATE TABLE `{0}` (id int); INSERT INTO `{0}` VALUES (1);",
                tbl_name
            ))
            .await?;
    }

    let ctx = TestHandle::start_noria(url.to_string()).await?;
    ctx.ready_notify.as_ref().unwrap().notified().await;

    for t in 0..TOTAL_TABLES {
        // Just check that all of the tables are really there
        let tbl_name = format!("t{}", t);
        ctx.controller().await.table(&tbl_name).await.unwrap();
    }

    ctx.stop().await;

    for t in 0..TOTAL_TABLES {
        let tbl_name = format!("t{}", t);
        client
            .query(&format!("DROP TABLE IF EXISTS `{0}` CASCADE;", tbl_name))
            .await?;
    }

    client.stop().await;

    Ok(())
}

async fn mysql_datetime_replication_inner() -> ReadySetResult<()> {
    let url = &mysql_url();
    let mut client = DbConnection::connect(url).await?;
    client
        .query(
            "
            DROP TABLE IF EXISTS `dt_test` CASCADE;
            DROP VIEW IF EXISTS dt_test_view;
            CREATE TABLE `dt_test` (
                id int NOT NULL PRIMARY KEY,
                dt datetime,
                ts timestamp,
                d date,
                t time
            );
            CREATE VIEW dt_test_view AS SELECT * FROM `dt_test` ORDER BY id ASC",
        )
        .await?;

    // Allow invalid values for dates
    client.query("SET @@sql_mode := ''").await?;
    client
        .query(
            "INSERT INTO `dt_test` VALUES
                (0, '0000-00-00', '0000-00-00', '0000-00-00', '25:27:89'),
                (1, '0002-00-00', '0020-00-00', '0200-00-00', '14:27:89')",
        )
        .await?;

    let mut ctx = TestHandle::start_noria(url.to_string()).await?;
    ctx.ready_notify.as_ref().unwrap().notified().await;

    // TODO: Those are obviously not the right answers, but at least we don't panic
    ctx.check_results(
        "dt_test_view",
        "Snapshot",
        &[
            &[
                D::Int(0),
                D::None,
                D::None,
                D::None,
                D::Time(MysqlTime::from_hmsus(false, 0, 0, 0, 0).into()),
            ],
            &[
                D::Int(1),
                D::None,
                D::None,
                D::None,
                D::Time(MysqlTime::from_hmsus(false, 0, 0, 0, 0).into()),
            ],
        ],
    )
    .await?;

    // Repeat, but this time using binlog replication
    client
        .query(
            "INSERT INTO `dt_test` VALUES
                (2, '0000-00-00', '0000-00-00', '0000-00-00', '25:27:89'),
                (3, '0002-00-00', '0020-00-00', '0200-00-00', '14:27:89')",
        )
        .await?;

    ctx.check_results(
        "dt_test_view",
        "Replication",
        &[
            &[
                D::Int(0),
                D::None,
                D::None,
                D::None,
                D::Time(MysqlTime::from_hmsus(false, 0, 0, 0, 0).into()),
            ],
            &[
                D::Int(1),
                D::None,
                D::None,
                D::None,
                D::Time(MysqlTime::from_hmsus(false, 0, 0, 0, 0).into()),
            ],
            &[
                D::Int(2),
                D::None,
                D::None,
                D::None,
                D::Time(MysqlTime::from_hmsus(false, 0, 0, 0, 0).into()),
            ],
            &[
                D::Int(3),
                D::None,
                D::None,
                D::None,
                D::Time(MysqlTime::from_hmsus(false, 0, 0, 0, 0).into()),
            ],
        ],
    )
    .await?;

    client.stop().await;
    ctx.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn mysql_datetime_replication() -> ReadySetResult<()> {
    mysql_datetime_replication_inner().await
}
