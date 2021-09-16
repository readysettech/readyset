use mysql_async::prelude::Queryable;
use noria::DataType as D;
use noria::{
    consensus::{Authority, LocalAuthority, LocalAuthorityStore},
    ControllerHandle, DataType, ReadySetResult,
};
use noria_server::Builder;
use replicators::NoriaAdapter;
use std::env;
use std::sync::Arc;

const CREATE_SCHEMA: &str = "
    DROP TABLE IF EXISTS noria CASCADE;
    DROP VIEW IF EXISTS noria_view;
    CREATE TABLE noria (
        id int NOT NULL PRIMARY KEY,
        string varchar(20) NOT NULL,
        bignum int
    );
    CREATE VIEW noria_view AS SELECT id,string,bignum FROM noria ORDER BY id ASC";

const POPULATE_SCHEMA: &str =
    "INSERT INTO noria VALUES (1, 'abc', 2), (2, 'bcd', 3), (40, 'xyz', 4)";

/// A convinience init to convert 3 character byte slice to TinyText noria type
const fn tiny(text: &[u8]) -> DataType {
    let mut arr = [0u8; 15];
    arr[0] = text[0];
    arr[1] = text[1];
    arr[2] = text[2];
    D::TinyText(arr)
}

const SNAPSHOT_RESULT: &[&[DataType]] = &[
    &[D::Int(1), tiny(b"abc"), D::Int(2)],
    &[D::Int(2), tiny(b"bcd"), D::Int(3)],
    &[D::Int(40), tiny(b"xyz"), D::Int(4)],
];

const TESTS: &[(&str, &str, &[&[DataType]])] = &[
    (
        "Test UPDATE key column replication",
        "UPDATE noria SET id=id+10",
        &[
            &[D::Int(11), tiny(b"abc"), D::Int(2)],
            &[D::Int(12), tiny(b"bcd"), D::Int(3)],
            &[D::Int(50), tiny(b"xyz"), D::Int(4)],
        ],
    ),
    (
        "Test DELETE replication",
        "DELETE FROM noria WHERE string='bcd'",
        &[
            &[D::Int(11), tiny(b"abc"), D::Int(2)],
            &[D::Int(50), tiny(b"xyz"), D::Int(4)],
        ],
    ),
    (
        "Test INSERT replication",
        "INSERT INTO noria VALUES (1, 'abc', 2), (2, 'bcd', 3), (40, 'xyz', 4)",
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
        "UPDATE noria SET bignum=id+10",
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
const DISCONNECT_QUERY: &str = "INSERT INTO noria VALUES (3, 'abc', 2), (5, 'xyz', 4)";
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
                c.simple_query(query).await?;
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
        let noria = Builder::for_tests()
            .start(Arc::clone(&authority))
            .await
            .unwrap();

        let mut handle = TestHandle {
            url,
            noria,
            authority,
            replication_rt: None,
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
        ));

        if let Some(rt) = self.replication_rt.replace(runtime) {
            rt.shutdown_background();
        }

        Ok(())
    }

    async fn check_results(
        &mut self,
        test_name: &str,
        test_results: &[&[DataType]],
    ) -> ReadySetResult<()> {
        let mut getter = self.controller().await.view("noria_view").await?;
        let results = getter.lookup(&[0.into()], true).await?;
        let mut results = results.as_ref().to_owned();
        results.sort(); // Simple `lookup` does not sort the results, so we just sort them ourselves
        assert_eq!(results, *test_results, "{} incorrect", test_name);
        Ok(())
    }
}

async fn replication_test_inner(url: &str) -> ReadySetResult<()> {
    let mut client = DbConnection::connect(url).await?;
    client.query(CREATE_SCHEMA).await?;
    client.query(POPULATE_SCHEMA).await?;

    let mut ctx = TestHandle::start_noria(url.to_string()).await?;
    // Allow some time to snapshot
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    ctx.check_results("Snapshot", SNAPSHOT_RESULT).await?;

    for (test_name, test_query, test_results) in TESTS {
        client.query(test_query).await?;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        ctx.check_results(test_name, *test_results).await?;
    }

    // Stop the replication task, issue some queries then check they are picked up after reconnect
    ctx.stop_repl().await;
    client.query(DISCONNECT_QUERY).await?;

    // Make sure no replication takes place for real
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    ctx.check_results("Disconnected", TESTS[TESTS.len() - 1].2)
        .await?;

    // Resume replication
    ctx.start_repl().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await; // Need time to sync again
    ctx.check_results("Reconnect", RECONNECT_RESULT).await?;

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
#[ignore] // FIXME: this is flaky and can't be run locally for reasons I don't understand right now
async fn pgsql_replication() -> ReadySetResult<()> {
    replication_test_inner(&pgsql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_replication() -> ReadySetResult<()> {
    replication_test_inner(&mysql_url()).await
}
