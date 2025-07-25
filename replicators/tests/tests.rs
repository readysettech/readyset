use std::env;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use database_utils::{DatabaseURL, ReplicationServerId, UpstreamConfig as Config};
use itertools::Itertools;
use mysql_async::prelude::Queryable;
use mysql_time::MySqlTime;
use rand::distr::Alphanumeric;
use rand::{Rng, SeedableRng};
use readyset_client::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use readyset_client::recipe::changelist::{Change, ChangeList, CreateCache};
use readyset_client::ReadySetHandle;
use readyset_data::{Collation, DfValue, Dialect, TimestampTz, TinyText};
use readyset_errors::{internal, internal_err, ReadySetError, ReadySetResult};
use readyset_server::Builder;
use readyset_server::NodeIndex;
use readyset_sql::ast::{NonReplicatedRelation, Relation};
use readyset_sql_parsing::{parse_select, ParsingPreset};
use readyset_telemetry_reporter::{TelemetryEvent, TelemetryInitializer, TelemetrySender};
use readyset_util::eventually;
use readyset_util::shutdown::ShutdownSender;
use replicators::db_util::error_is_slot_not_found;
use replicators::table_filter::TableFilter;
use replicators::{ControllerMessage, NoriaAdapter, ReplicatorMessage};
use test_utils::tags;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::sleep;
use tokio_postgres::error::SqlState;
use tracing::{debug, error, trace};

const MAX_ATTEMPTS: usize = 40;

// Postgres does not accept MySQL escapes, so rename the table before the query
const PGSQL_RENAME: (&str, &str) = ("`groups`", "groups");

const CREATE_SCHEMA: &str = "
    DROP TABLE IF EXISTS `groups` CASCADE;
    DROP VIEW IF EXISTS noria_view;
    CREATE TABLE `groups` (
        id int NOT NULL PRIMARY KEY,
        string varchar(20),
        bignum int
    );
    CREATE VIEW noria_view AS SELECT id,string,bignum FROM `groups` ORDER BY id ASC";

const POPULATE_SCHEMA: &str =
    "INSERT INTO `groups` VALUES (1, 'abc', 2), (2, 'bcd', 3), (3, NULL, NULL), (40, 'xyz', 4)";

/// A convenience init to convert 3 character byte slice to TinyText noria type
const fn tiny<const N: usize>(text: &[u8; N]) -> DfValue {
    DfValue::TinyText(TinyText::from_arr(text))
}

const SNAPSHOT_RESULT: &[&[DfValue]] = &[
    &[DfValue::Int(1), tiny(b"abc"), DfValue::Int(2)],
    &[DfValue::Int(2), tiny(b"bcd"), DfValue::Int(3)],
    &[DfValue::Int(3), DfValue::None, DfValue::None],
    &[DfValue::Int(40), tiny(b"xyz"), DfValue::Int(4)],
];

const TESTS: &[(&str, &str, &[&[DfValue]])] = &[
    (
        "Test UPDATE key column replication",
        "UPDATE `groups` SET id=id+10",
        &[
            &[DfValue::Int(11), tiny(b"abc"), DfValue::Int(2)],
            &[DfValue::Int(12), tiny(b"bcd"), DfValue::Int(3)],
            &[DfValue::Int(13), DfValue::None, DfValue::None],
            &[DfValue::Int(50), tiny(b"xyz"), DfValue::Int(4)],
        ],
    ),
    (
        "Test DELETE replication",
        "DELETE FROM `groups` WHERE string='bcd'",
        &[
            &[DfValue::Int(11), tiny(b"abc"), DfValue::Int(2)],
            &[DfValue::Int(13), DfValue::None, DfValue::None],
            &[DfValue::Int(50), tiny(b"xyz"), DfValue::Int(4)],
        ],
    ),
    (
        "Test INSERT replication",
        "INSERT INTO `groups` VALUES (1, 'abc', 2), (2, 'bcd', 3), (40, 'xyz', 4)",
        &[
            &[DfValue::Int(1), tiny(b"abc"), DfValue::Int(2)],
            &[DfValue::Int(2), tiny(b"bcd"), DfValue::Int(3)],
            &[DfValue::Int(11), tiny(b"abc"), DfValue::Int(2)],
            &[DfValue::Int(13), DfValue::None, DfValue::None],
            &[DfValue::Int(40), tiny(b"xyz"), DfValue::Int(4)],
            &[DfValue::Int(50), tiny(b"xyz"), DfValue::Int(4)],
        ],
    ),
    (
        "Test UPDATE non-key column replication",
        "UPDATE `groups` SET bignum=id+10",
        &[
            &[DfValue::Int(1), tiny(b"abc"), DfValue::Int(11)],
            &[DfValue::Int(2), tiny(b"bcd"), DfValue::Int(12)],
            &[DfValue::Int(11), tiny(b"abc"), DfValue::Int(21)],
            &[DfValue::Int(13), DfValue::None, DfValue::Int(23)],
            &[DfValue::Int(40), tiny(b"xyz"), DfValue::Int(50)],
            &[DfValue::Int(50), tiny(b"xyz"), DfValue::Int(60)],
        ],
    ),
];

/// Test query we issue after replicator disconnect
const DISCONNECT_QUERY: &str = "INSERT INTO `groups` VALUES (3, 'abc', 2), (5, 'xyz', 4)";
/// Test result after replicator reconnects and catches up
const RECONNECT_RESULT: &[&[DfValue]] = &[
    &[DfValue::Int(1), tiny(b"abc"), DfValue::Int(11)],
    &[DfValue::Int(2), tiny(b"bcd"), DfValue::Int(12)],
    &[DfValue::Int(3), tiny(b"abc"), DfValue::Int(2)],
    &[DfValue::Int(5), tiny(b"xyz"), DfValue::Int(4)],
    &[DfValue::Int(11), tiny(b"abc"), DfValue::Int(21)],
    &[DfValue::Int(13), DfValue::None, DfValue::Int(23)],
    &[DfValue::Int(40), tiny(b"xyz"), DfValue::Int(50)],
    &[DfValue::Int(50), tiny(b"xyz"), DfValue::Int(60)],
];

/// Channel used to receive notifications from the replicator. Contains the receiver since the
/// replicator takes a reference to the sender.
struct TestChannel(UnboundedReceiver<ControllerMessage>);

impl TestChannel {
    /// Creates a new mock controller channel. Also returns a static reference to the sender so
    /// that it can be moved into tokio::spawn.
    pub fn new() -> (&'static mut UnboundedSender<ControllerMessage>, Self) {
        let (controller_tx, controller_rx) = tokio::sync::mpsc::unbounded_channel();
        let controller_tx = Box::leak(Box::new(controller_tx));
        (controller_tx, Self(controller_rx))
    }

    /// Returns after receiving `ControllerMessage::SnapshotDone`. Errors on receiving any other
    /// message, which should not happen if this is called when waiting for snapshotting to
    /// complete. Will timeout after the duration specified by the SNAPSHOT_TIMEOUT_MS
    /// environment variable (defaults to 60 seconds).
    async fn snapshot_completed(&mut self) -> ReadySetResult<()> {
        let timeout_ms = std::env::var("SNAPSHOT_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(120000);

        tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), self.0.recv())
            .await
            .map_err(|_| {
                internal_err!(
                    "Exceeded timeout of SNAPSHOT_TIMEOUT_MS={} waiting for snapshot to complete",
                    timeout_ms
                )
            })?
            .ok_or_else(|| internal_err!("Did not receive snapshot controller message"))
            .map(|msg| match msg {
                ControllerMessage::SnapshotDone => Ok(()),
                ControllerMessage::UnrecoverableError(e) => Err(e),
                _ => internal!("Unexpected controller message while waiting for snapshot: {msg:?}"),
            })?
    }
}

struct TestHandle {
    url: String,
    dialect: Dialect,
    noria: readyset_server::Handle,
    authority: Arc<Authority>,
    // We spin a whole runtime for the replication task because the tokio postgres
    // connection spawns a background task we can only terminate by dropping the runtime
    replication_rt: Option<tokio::runtime::Runtime>,
    controller_rx: Option<TestChannel>,
    replicator_tx: Option<UnboundedSender<ReplicatorMessage>>,
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
            let test_db_name = opts.db_name().unwrap();
            let no_db_opts = mysql_async::OptsBuilder::from_opts(opts.clone())
                .db_name::<String>(None)
                .prefer_socket(false);

            // First, connect without a db
            let mut client = mysql_async::Conn::new(no_db_opts).await?;

            // Then drop and recreate the test db
            client
                .query_drop(format!("DROP SCHEMA IF EXISTS {test_db_name};"))
                .await?;
            client
                .query_drop(format!("CREATE SCHEMA {test_db_name};"))
                .await?;

            // Then switch to the test db
            client.query_drop(format!("USE {test_db_name};")).await?;

            Ok(DbConnection::MySQL(client))
        } else if url.starts_with("postgresql") {
            let opts = tokio_postgres::Config::from_str(url)?;

            // Drop and recreate the test db
            {
                let test_db_name = opts.get_dbname().unwrap();
                let mut no_db_opts = opts.clone();
                no_db_opts.dbname("postgres");
                let (no_db_client, conn) = no_db_opts.connect(tokio_postgres::NoTls).await?;
                tokio::spawn(conn);

                loop {
                    match no_db_client
                        .simple_query(&format!("DROP DATABASE IF EXISTS {test_db_name}"))
                        .await
                    {
                        Ok(_) => break,
                        Err(e) => {
                            if let Some(db_err) = e.as_db_error() {
                                if *db_err.code() == SqlState::OBJECT_IN_USE {
                                    debug!(
                                        "Waiting for database \"{test_db_name}\" to not be in use"
                                    );
                                    sleep(Duration::from_millis(100)).await;
                                    continue;
                                }
                            }
                            return Err(e.into());
                        }
                    };
                }
                no_db_client
                    .simple_query(&format!("CREATE DATABASE {test_db_name}"))
                    .await?;
            }

            let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
                .await
                .unwrap();
            let connection_handle = tokio::spawn(async move { conn.await.map_err(Into::into) });
            client
                .simple_query(&format!(
                    "CREATE SCHEMA IF NOT EXISTS {}",
                    opts.get_dbname().unwrap()
                ))
                .await?;
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
    async fn start_noria(
        url: String,
        config: Option<Config>,
    ) -> ReadySetResult<(TestHandle, ShutdownSender)> {
        Self::start_noria_with_builder(url, config, Builder::for_tests()).await
    }

    async fn start_noria_with_builder(
        url: String,
        config: Option<Config>,
        builder: Builder,
    ) -> ReadySetResult<(TestHandle, ShutdownSender)> {
        let authority_store = Arc::new(LocalAuthorityStore::new());
        let authority = Arc::new(Authority::from(LocalAuthority::new_with_store(
            authority_store,
        )));
        TestHandle::start_with_authority(url, authority, config, builder).await
    }

    async fn start_with_authority(
        url: String,
        authority: Arc<Authority>,
        config: Option<Config>,
        mut builder: Builder,
    ) -> ReadySetResult<(TestHandle, ShutdownSender)> {
        readyset_tracing::init_test_logging();
        let persistence = readyset_server::PersistenceParameters {
            mode: readyset_server::DurabilityMode::DeleteOnExit,
            ..Default::default()
        };
        builder.set_persistence(persistence);
        let parsing_preset = builder.parsing_preset;
        let telemetry_sender = builder.telemetry.clone();
        let (noria, shutdown_tx) = builder.start(Arc::clone(&authority)).await.unwrap();

        let dialect = if url.starts_with("postgresql") {
            Dialect::DEFAULT_POSTGRESQL
        } else {
            Dialect::DEFAULT_MYSQL
        };

        let mut handle = TestHandle {
            url,
            dialect,
            noria,
            authority,
            replication_rt: None,
            controller_rx: None,
            replicator_tx: None,
        };
        handle.replicator_tx = Some(
            handle
                .start_repl(config, telemetry_sender, true, parsing_preset)
                .await?,
        );

        Ok((handle, shutdown_tx))
    }

    async fn controller(&self) -> ReadySetHandle {
        ReadySetHandle::new(Arc::clone(&self.authority)).await
    }

    async fn stop(mut self) {
        self.stop_repl().await;
    }

    async fn stop_repl(&mut self) {
        if let Some(rt) = self.replication_rt.take() {
            rt.shutdown_background();
        }
    }

    async fn start_repl(
        &mut self,
        config: Option<Config>,
        telemetry_sender: TelemetrySender,
        server_startup: bool,
        parsing_preset: ParsingPreset,
    ) -> ReadySetResult<tokio::sync::mpsc::UnboundedSender<ReplicatorMessage>> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let controller = ReadySetHandle::new(Arc::clone(&self.authority)).await;

        let (controller_tx, controller_rx) = TestChannel::new();
        let (replicator_tx, mut replicator_rx): (
            UnboundedSender<ReplicatorMessage>,
            UnboundedReceiver<ReplicatorMessage>,
        ) = tokio::sync::mpsc::unbounded_channel();
        self.controller_rx = Some(controller_rx);
        let (replication_tables, replication_tables_ignore) = if let Some(config) = &config {
            (
                config.replication_tables.as_deref(),
                config.replication_tables_ignore.as_deref(),
            )
        } else {
            (None, None)
        };
        let url: DatabaseURL = self.url.parse().unwrap();
        let mut table_filter = TableFilter::try_new(
            url.dialect(),
            replication_tables,
            replication_tables_ignore,
            match url.dialect() {
                readyset_sql::Dialect::MySQL => url.db_name(),
                readyset_sql::Dialect::PostgreSQL => None,
            },
        )?;
        let upstream_db_url = Some(self.url.clone().into());
        let cdc_db_url = Some(self.url.clone().into());
        runtime.spawn(async move {
            let Err(error) = NoriaAdapter::start(
                controller,
                &Config {
                    upstream_db_url,
                    cdc_db_url,
                    ..config.unwrap_or_default()
                },
                &url,
                &mut table_filter,
                controller_tx,
                &mut replicator_rx,
                telemetry_sender,
                server_startup,
                false, // disable statement logging in tests
                parsing_preset,
            )
            .await;
            error!(%error, "Error in replicator");
            let _ = controller_tx.send(ControllerMessage::UnrecoverableError(error));
        });

        // keep at least one sender open
        let replicator_tx_clone = replicator_tx.clone();
        runtime.spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                if replicator_tx_clone.is_closed() {
                    break;
                }
            }
        });

        if let Some(rt) = self.replication_rt.replace(runtime) {
            rt.shutdown_background();
        }

        Ok(replicator_tx)
    }

    async fn get_results(
        &mut self,
        view_name: &str,
        expected_results: &[&[DfValue]],
    ) -> ReadySetResult<Vec<Vec<DfValue>>> {
        let mut attempt: usize = 0;
        let result = loop {
            match self.get_results_inner(view_name).await {
                Err(_) if attempt < MAX_ATTEMPTS => {
                    // Sometimes things are slow in CI, so we retry a few times before giving up
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Ok(res) if res != expected_results && attempt < MAX_ATTEMPTS => {
                    // Sometimes things are slow in CI, so we retry a few times before giving up
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Ok(res) => {
                    break res;
                }
                Err(err) => return Err(err),
            }
        };
        Ok(result)
    }

    async fn get_results_inner(&mut self, view_name: &str) -> ReadySetResult<Vec<Vec<DfValue>>> {
        let query_name = format!("q_{view_name}");
        let select_stmt = format!("SELECT * FROM public.{view_name}");
        self.controller()
            .await
            .extend_recipe(ChangeList::from_changes(
                vec![
                    Change::Drop {
                        name: Relation {
                            schema: Some("public".into()),
                            name: query_name.clone().into(),
                        },
                        if_exists: true,
                    },
                    Change::CreateCache(CreateCache {
                        name: Some(Relation {
                            schema: Some("public".into()),
                            name: query_name.clone().into(),
                        }),
                        statement: Box::new(
                            parse_select(readyset_sql::Dialect::MySQL, select_stmt.clone())
                                .unwrap(),
                        ),
                        always: false,
                    }),
                ],
                self.dialect,
            ))
            .await?;
        let mut getter = self
            .controller()
            .await
            .view(Relation {
                schema: Some("public".into()),
                name: query_name.into(),
            })
            .await?
            .into_reader_handle()
            .unwrap();
        let results = getter.lookup(&[0.into()], true).await?;
        let mut results = results.into_vec();
        results.sort(); // Simple `lookup` does not sort the results, so we just sort them ourselves
        Ok(results)
    }

    async fn assert_table_exists(&mut self, schema: &str, name: &str) {
        self.noria
            .table(Relation {
                schema: Some(schema.into()),
                name: name.into(),
            })
            .await
            .unwrap();
    }

    async fn assert_table_missing(&mut self, schema: &str, name: &str) {
        self.noria
            .table(Relation {
                schema: Some(schema.into()),
                name: name.into(),
            })
            .await
            .unwrap_err();
    }
}

macro_rules! check_results {
    ($ctx:ident, $view:expr, $test_name:expr, $expected_results:expr $(,)?) => {
        let expected_results: &[&[DfValue]] = $expected_results;
        let result = $ctx.get_results($view, expected_results).await.unwrap();
        let result_slices = result
            .iter()
            .map(|row| row.as_slice())
            .collect::<Vec<&[DfValue]>>();
        pretty_assertions::assert_eq!(
            expected_results,
            result_slices.as_slice(),
            "test {} failed",
            $test_name
        );
    };
}

/// Tests that we can have multiple ReadySet instances connected to the same postgres upstream
/// without replication slot issues.
async fn replication_test_multiple(url: &str) -> ReadySetResult<()> {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await?;
    client.query(CREATE_SCHEMA).await?;
    client.query(POPULATE_SCHEMA).await?;

    let config_one = Config {
        replication_server_id: Some(ReplicationServerId("1".into())),
        ..Default::default()
    };
    let config_two = Config {
        replication_server_id: Some(ReplicationServerId("2".into())),
        ..Default::default()
    };
    let (mut ctx_one, shutdown_tx_one) =
        TestHandle::start_noria(url.to_string(), Some(config_one)).await?;
    let (mut ctx_two, shutdown_tx_two) =
        TestHandle::start_noria(url.to_string(), Some(config_two)).await?;

    for ctx in [&mut ctx_one, &mut ctx_two] {
        ctx.controller_rx
            .as_mut()
            .unwrap()
            .snapshot_completed()
            .await
            .unwrap();

        check_results!(ctx, "noria_view", "Snapshot", SNAPSHOT_RESULT);
    }

    client.stop().await;
    tokio::join!(shutdown_tx_one.shutdown(), shutdown_tx_two.shutdown());

    Ok(())
}

async fn replication_test_inner(url: &str) {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();
    client.query(CREATE_SCHEMA).await.unwrap();
    client.query(POPULATE_SCHEMA).await.unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(ctx, "noria_view", "Snapshot", SNAPSHOT_RESULT);

    for (test_name, test_query, test_results) in TESTS {
        client.query(test_query).await.unwrap();
        check_results!(ctx, "noria_view", test_name, test_results);
    }

    // Stop the replication task, issue some queries then check they are picked up after reconnect
    ctx.stop_repl().await;
    client.query(DISCONNECT_QUERY).await.unwrap();

    // Make sure no replication takes place for real
    check_results!(ctx, "noria_view", "Disconnected", TESTS[TESTS.len() - 1].2);

    // Resume replication
    ctx.start_repl(
        None,
        TelemetrySender::new_no_op(),
        false,
        // TODO(mvzink): Does this need to be configurable by (these) tests?
        ParsingPreset::BothPanicOnMismatch,
    )
    .await
    .unwrap();
    check_results!(ctx, "noria_view", "Reconnect", RECONNECT_RESULT);

    client.stop().await;
    ctx.stop().await;

    shutdown_tx.shutdown().await;
}

fn pgsql_url() -> String {
    format!(
        "postgresql://postgres:noria@{}:{}/noria",
        env::var("PGHOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("PGPORT").unwrap_or_else(|_| "5432".into()),
    )
}

fn pgsql13_url() -> String {
    format!(
        "postgresql://postgres:noria@{}:{}/noria",
        env::var("PGHOST13").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("PGPORT13").unwrap_or_else(|_| "5433".into()),
    )
}

fn mysql_url() -> String {
    format!(
        "mysql://root:noria@{}:{}/public",
        env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        env::var("MYSQL_TCP_PORT").unwrap_or_else(|_| "3306".into()),
    )
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_replication() {
    replication_test_inner(&pgsql_url()).await
}

/// Tests multiple readyset instances pointed at the same postgres upstream to verify that multiple
/// readyset instances can replicate off the same upstream.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
#[ignore = "Flaky test (REA-3061)"]
async fn pgsql_replication_multiple() {
    replication_test_multiple(&pgsql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication_multiple() {
    replication_test_multiple(&mysql_url()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication() {
    replication_test_inner(&mysql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_replication_catch_up() {
    replication_catch_up_inner(&pgsql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication_catch_up() {
    replication_catch_up_inner(&mysql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_replication_many_tables() {
    replication_many_tables_inner(&pgsql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication_many_tables() {
    replication_many_tables_inner(&mysql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_replication_big_tables() {
    replication_big_tables_inner(&pgsql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication_big_tables() {
    replication_big_tables_inner(&mysql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_datetime_replication() {
    mysql_datetime_replication_inner().await.unwrap();
}

async fn mysql_binary_collation_padding_inner() {
    let url = &mysql_url();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query(
            "
            DROP TABLE IF EXISTS `col_bin_pad` CASCADE;
            CREATE TABLE `col_bin_pad` (
                id int NOT NULL PRIMARY KEY,
                c BINARY(3)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            INSERT INTO `col_bin_pad` VALUES (1, 'ࠈ');
            INSERT INTO `col_bin_pad` VALUES (2, 'A');
            INSERT INTO `col_bin_pad` VALUES (3, 'AAA');
            INSERT INTO `col_bin_pad` VALUES (4, '¥');",
        )
        .await
        .unwrap();
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    check_results!(
        ctx,
        "col_bin_pad",
        "Snapshot",
        &[
            &[
                DfValue::Int(1),
                // 'ࠈ' is the UTF-8 encoding of U+E0A088
                DfValue::ByteArray(vec![0xE0, 0xA0, 0x88].into()),
            ],
            &[
                DfValue::Int(2),
                DfValue::ByteArray(vec![0x41, 0x0, 0x0].into()),
            ],
            &[
                DfValue::Int(3),
                DfValue::ByteArray(vec![0x41, 0x41, 0x41].into()),
            ],
            &[
                DfValue::Int(4),
                // '¥' is the UTF-8 encoding of U+C2A5
                DfValue::ByteArray(vec![0xC2, 0xA5, 0x0].into()),
            ],
        ],
    );

    // Replication and mix of characters from 1st and 2rd byte on the same row
    client
        .query(
            "
        INSERT INTO `col_bin_pad` VALUES (5, 'B¥');
        INSERT INTO `col_bin_pad` VALUES (6, 'B');
        INSERT INTO `col_bin_pad` VALUES (7, 'BBB');
        INSERT INTO `col_bin_pad` VALUES (8, '¥');
        ",
        )
        .await
        .unwrap();
    check_results!(
        ctx,
        "col_bin_pad",
        "Replication",
        &[
            &[
                DfValue::Int(1),
                // 'ࠈ' is the UTF-8 encoding of U+E0A088
                DfValue::ByteArray(vec![0xE0, 0xA0, 0x88].into()),
            ],
            &[
                DfValue::Int(2),
                DfValue::ByteArray(vec![0x41, 0x0, 0x0].into()),
            ],
            &[
                DfValue::Int(3),
                DfValue::ByteArray(vec![0x41, 0x41, 0x41].into()),
            ],
            &[
                DfValue::Int(4),
                // '¥' is the UTF-8 encoding of U+C2A5
                DfValue::ByteArray(vec![0xC2, 0xA5, 0x0].into()),
            ],
            &[
                DfValue::Int(5),
                // '¥' is the UTF-8 encoding of U+C2A5
                DfValue::ByteArray(vec![0x42, 0xC2, 0xA5].into()),
            ],
            &[
                DfValue::Int(6),
                DfValue::ByteArray(vec![0x42, 0x0, 0x0].into()),
            ],
            &[
                DfValue::Int(7),
                DfValue::ByteArray(vec![0x42, 0x42, 0x42].into()),
            ],
            &[
                DfValue::Int(8),
                // '¥' is the UTF-8 encoding of U+C2A5
                DfValue::ByteArray(vec![0xC2, 0xA5, 0x0].into()),
            ],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

async fn mysql_char_collation_padding_inner() {
    let url = &mysql_url();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query(
            "
            DROP TABLE IF EXISTS `col_pad` CASCADE;
            CREATE TABLE `col_pad` (
                id int NOT NULL PRIMARY KEY,
                c CHAR(3)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            INSERT INTO `col_pad` VALUES (1, 'ࠈࠈ');
            INSERT INTO `col_pad` VALUES (2, 'A');
            INSERT INTO `col_pad` VALUES (3, 'AAA');
            INSERT INTO `col_pad` (id) VALUES (4);",
        )
        .await
        .unwrap();
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    check_results!(
        ctx,
        "col_pad",
        "Snapshot",
        &[
            &[
                DfValue::Int(1),
                // 'ࠈࠈ ' is the UTF-8 encoding of U+E0A088 U+E0A088 U+20
                DfValue::TinyText(
                    TinyText::from_slice(
                        vec![0xE0, 0xA0, 0x88, 0xE0, 0xA0, 0x88, 0x20].as_slice(),
                        Collation::Utf8
                    )
                    .unwrap_or_else(|_| TinyText::from_arr(b"")),
                ),
            ],
            &[
                DfValue::Int(2),
                DfValue::TinyText(TinyText::from_arr(b"A  ")),
            ],
            &[
                DfValue::Int(3),
                DfValue::TinyText(TinyText::from_arr(b"AAA")),
            ],
            &[DfValue::Int(4), DfValue::None],
        ],
    );

    // Replication and mix of characters from 1st and 3rd byte on the same row
    client
        .query(
            "
        INSERT INTO `col_pad` VALUES (5, 'Bࠉ');
        INSERT INTO `col_pad` VALUES (6, 'B');
        INSERT INTO `col_pad` VALUES (7, 'BBB');
        INSERT INTO `col_pad` (id) VALUES (8);
        ",
        )
        .await
        .unwrap();
    check_results!(
        ctx,
        "col_pad",
        "Replication",
        &[
            &[
                DfValue::Int(1),
                // 'ࠈࠈ ' is the UTF-8 encoding of U+E0A088 U+E0A088 U+20
                DfValue::TinyText(
                    TinyText::from_slice(
                        vec![0xE0, 0xA0, 0x88, 0xE0, 0xA0, 0x88, 0x20].as_slice(),
                        Collation::Utf8
                    )
                    .unwrap_or_else(|_| TinyText::from_arr(b"")),
                ),
            ],
            &[
                DfValue::Int(2),
                DfValue::TinyText(TinyText::from_arr(b"A  ")),
            ],
            &[
                DfValue::Int(3),
                DfValue::TinyText(TinyText::from_arr(b"AAA")),
            ],
            &[DfValue::Int(4), DfValue::None],
            &[
                DfValue::Int(5),
                // 'Bࠉ ' is the UTF-8 encoding of U+E42 U+E0A089 U+20
                DfValue::TinyText(
                    TinyText::from_slice(
                        vec![0x42, 0xE0, 0xA0, 0x89, 0x20].as_slice(),
                        Collation::Utf8
                    )
                    .unwrap_or_else(|_| TinyText::from_arr(b"")),
                ),
            ],
            &[
                DfValue::Int(6),
                DfValue::TinyText(TinyText::from_arr(b"B  ")),
            ],
            &[
                DfValue::Int(7),
                DfValue::TinyText(TinyText::from_arr(b"BBB")),
            ],
            &[DfValue::Int(8), DfValue::None],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_binary_collation_padding() {
    mysql_binary_collation_padding_inner().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_char_collation_padding() {
    mysql_char_collation_padding_inner().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres15_upstream)]
async fn pgsql_replication_filter() {
    replication_filter_inner(&pgsql_url()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication_filter() {
    replication_filter_inner(&mysql_url()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_replication_all_schemas() {
    replication_all_schemas_inner(&pgsql_url()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication_all_schemas() {
    replication_all_schemas_inner(&mysql_url()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_replication_resnapshot() {
    resnapshot_inner(&pgsql_url()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replication_resnapshot() {
    resnapshot_inner(&mysql_url()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql14_ddl_replicate_drop_table() {
    postgresql_ddl_replicate_drop_table_internal(&pgsql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql13_ddl_replicate_drop_table() {
    postgresql_ddl_replicate_drop_table_internal(&pgsql13_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql14_ddl_replicate_create_table() {
    postgresql_ddl_replicate_create_table_internal(&pgsql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql13_ddl_replicate_create_table() {
    postgresql_ddl_replicate_create_table_internal(&pgsql13_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql14_ddl_replicate_drop_view() {
    postgresql_ddl_replicate_drop_view_internal(&pgsql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql13_ddl_replicate_drop_view() {
    postgresql_ddl_replicate_drop_view_internal(&pgsql13_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql14_ddl_replicate_create_view() {
    postgresql_ddl_replicate_create_view_internal(&pgsql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn psql13_ddl_replicate_create_view() {
    postgresql_ddl_replicate_create_view_internal(&pgsql13_url()).await
}

/// This test checks that when writes and replication happen in parallel
/// noria correctly catches up from binlog
/// NOTE: If this test flakes, please notify Vlad
async fn replication_catch_up_inner(url: &str) -> ReadySetResult<()> {
    const TOTAL_INSERTS: usize = 5000;
    static INSERTS_DONE: AtomicUsize = AtomicUsize::new(0);

    let mut client = DbConnection::connect(url).await?;
    client
        .query(
            "
            DROP TABLE IF EXISTS catch_up CASCADE;
            DROP TABLE IF EXISTS catch_up_pk CASCADE;
            DROP VIEW IF EXISTS catch_up_view;
            DROP VIEW IF EXISTS catch_up_pk_view;
            CREATE TABLE catch_up (
                id int,
                val varchar(255)
            );
            CREATE TABLE catch_up_pk (
                id int PRIMARY KEY,
                val varchar(255)
            );
            CREATE VIEW catch_up_view AS SELECT * FROM catch_up;
            CREATE VIEW catch_up_pk_view AS SELECT * FROM catch_up_pk;",
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
                    .query("INSERT INTO catch_up VALUES (100, 'I am a teapot')")
                    .await?;

                client
                    .query(&format!(
                        "INSERT INTO catch_up_pk VALUES ({idx}, 'I am a teapot')"
                    ))
                    .await?;

                INSERTS_DONE.fetch_add(1, Ordering::Relaxed);
            }

            Ok(client)
        });

    while INSERTS_DONE.load(Ordering::Relaxed) < TOTAL_INSERTS / 5 {
        // Sleep a bit to let some writes happen first
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None).await?;
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    let mut client = inserter.await.unwrap()?;

    let rs: Vec<_> = std::iter::repeat_n(
        [DfValue::from(100), DfValue::from("I am a teapot")],
        TOTAL_INSERTS,
    )
    .collect();
    let rs: Vec<&[DfValue]> = rs.iter().map(|r| r.as_slice()).collect();
    check_results!(ctx, "catch_up_view", "Catch up", rs.as_slice());

    let rs: Vec<_> = (0..TOTAL_INSERTS)
        .map(|i| [DfValue::from(i as i32), DfValue::from("I am a teapot")])
        .collect();
    let rs: Vec<&[DfValue]> = rs.iter().map(|r| r.as_slice()).collect();
    check_results!(ctx, "catch_up_pk_view", "Catch up with pk", rs.as_slice());

    ctx.stop().await;

    client
        .query(
            "
        DROP TABLE IF EXISTS catch_up CASCADE;
        DROP TABLE IF EXISTS catch_up_pk CASCADE;
        DROP VIEW IF EXISTS catch_up_view;
        DROP VIEW IF EXISTS catch_up_pk_view;",
        )
        .await?;

    client.stop().await;

    shutdown_tx.shutdown().await;

    Ok(())
}

async fn replication_many_tables_inner(url: &str) -> ReadySetResult<()> {
    const TOTAL_TABLES: usize = 300;
    let mut client = DbConnection::connect(url).await?;

    for t in 0..TOTAL_TABLES {
        let tbl_name = format!("t{t}");
        client
            .query(&format!(
                "DROP TABLE IF EXISTS {tbl_name} CASCADE; CREATE TABLE {tbl_name} (id int); INSERT INTO {tbl_name} VALUES (1);"
            ))
            .await?;
    }

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None).await?;
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    for t in 0..TOTAL_TABLES {
        // Just check that all of the tables are really there
        ctx.controller()
            .await
            .table(Relation {
                schema: Some("public".into()),
                name: format!("t{t}").into(),
            })
            .await
            .unwrap();
    }

    ctx.stop().await;

    for t in 0..TOTAL_TABLES {
        client
            .query(&format!("DROP TABLE IF EXISTS t{t} CASCADE;"))
            .await?;
    }

    client.stop().await;

    shutdown_tx.shutdown().await;

    Ok(())
}

// This test will definitely trigger the global timeout if a session one is not set
async fn replication_big_tables_inner(url: &str) -> ReadySetResult<()> {
    const TOTAL_TABLES: usize = 2;
    const TOTAL_ROWS: usize = 10_000;

    let mut client = DbConnection::connect(url).await?;

    for t in 0..TOTAL_TABLES {
        let tbl_name = format!("t{t}");
        client
            .query(&format!(
                "DROP TABLE IF EXISTS {tbl_name} CASCADE; CREATE TABLE {tbl_name} (id int);",
            ))
            .await?;
        for r in 0..TOTAL_ROWS {
            client
                .query(&format!("INSERT INTO {tbl_name} VALUES ({r})"))
                .await?;
        }
    }

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None).await?;
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    for t in 0..TOTAL_TABLES {
        // Just check that all of the tables are really there
        ctx.controller()
            .await
            .table(Relation {
                schema: Some("public".into()),
                name: format!("t{t}").into(),
            })
            .await
            .unwrap();
    }

    ctx.stop().await;

    for t in 0..TOTAL_TABLES {
        client
            .query(&format!("DROP TABLE IF EXISTS t{t} CASCADE;"))
            .await?;
    }

    client.stop().await;

    shutdown_tx.shutdown().await;

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

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None).await?;
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // TODO: Those are obviously not the right answers, but at least we don't panic
    check_results!(
        ctx,
        "dt_test_view",
        "Snapshot",
        &[
            &[
                DfValue::Int(0),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::Time(MySqlTime::from_hmsus(false, 0, 0, 0, 0)),
            ],
            &[
                DfValue::Int(1),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::Time(MySqlTime::from_hmsus(false, 0, 0, 0, 0)),
            ],
        ],
    );

    // Repeat, but this time using binlog replication
    client
        .query(
            "INSERT INTO `dt_test` VALUES
                (2, '0000-00-00', '0000-00-00', '0000-00-00', '25:27:89'),
                (3, '0002-00-00', '0020-00-00', '0200-00-00', '14:27:89')",
        )
        .await?;

    check_results!(
        ctx,
        "dt_test_view",
        "Replication",
        &[
            &[
                DfValue::Int(0),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::Time(MySqlTime::from_hmsus(false, 0, 0, 0, 0)),
            ],
            &[
                DfValue::Int(1),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::Time(MySqlTime::from_hmsus(false, 0, 0, 0, 0)),
            ],
            &[
                DfValue::Int(2),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::Time(MySqlTime::from_hmsus(false, 0, 0, 0, 0)),
            ],
            &[
                DfValue::Int(3),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::TimestampTz(TimestampTz::zero()),
                DfValue::Time(MySqlTime::from_hmsus(false, 0, 0, 0, 0)),
            ],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;

    Ok(())
}

async fn replication_filter_inner(url: &str) -> ReadySetResult<()> {
    readyset_tracing::init_test_logging();

    let mut client = DbConnection::connect(url).await?;

    let cascade = if url.starts_with("postgresql") {
        "CASCADE"
    } else {
        ""
    };

    let query = format!(
        "
    DROP TABLE IF EXISTS t1 CASCADE; CREATE TABLE t1 (id int);
    DROP TABLE IF EXISTS t2 CASCADE; CREATE TABLE t2 (id int);
    DROP TABLE IF EXISTS t3 CASCADE; CREATE TABLE t3 (id int);
    DROP SCHEMA IF EXISTS noria2 {cascade}; CREATE SCHEMA noria2; CREATE TABLE noria2.t4 (id int);
    DROP SCHEMA IF EXISTS noria3 {cascade}; CREATE SCHEMA noria3;
    DROP VIEW IF EXISTS t1_view; CREATE VIEW t1_view AS SELECT * FROM t1;
    DROP VIEW IF EXISTS t2_view; CREATE VIEW t2_view AS SELECT * FROM t2;
    DROP VIEW IF EXISTS t3_view; CREATE VIEW t3_view AS SELECT * FROM t3;
    "
    );

    client.query(&query).await?;

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            replication_tables: Some("public.t3, public.t1, noria3.*, noria2.t4".to_string()),
            ..Default::default()
        }),
    )
    .await?;

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    ctx.assert_table_exists("public", "t1").await;
    ctx.assert_table_missing("public", "t2").await;
    ctx.assert_table_exists("public", "t3").await;
    ctx.assert_table_exists("noria2", "t4").await;

    let non_replicated_rels = ctx.noria.non_replicated_relations().await.unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t2".into(),
    };
    assert!(non_replicated_rels.contains(&NonReplicatedRelation::new(relation.clone()),));

    client
        .query(
            "
            CREATE TABLE noria2.t5 (id int);
            CREATE TABLE noria3.t6 (id int);
            INSERT INTO t1 VALUES (1),(2),(3);
            INSERT INTO t2 VALUES (1),(2),(3);
            INSERT INTO t3 VALUES (1),(2),(3);
            INSERT INTO noria2.t4 VALUES (1),(2),(3);
            ",
        )
        .await?;

    eventually! {
        let non_replicated_rels = ctx.noria.non_replicated_relations().await.unwrap();
        let relation =  Relation{schema: Some("noria2".into()),name: "t5".into()};
        non_replicated_rels.contains(&NonReplicatedRelation::new( relation))
    }

    ctx.noria
        .extend_recipe(
            ChangeList::from_strings(
                vec!["CREATE VIEW public.t4_view AS SELECT * FROM noria2.t4;"],
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    for view in ["t1_view", "t3_view", "t4_view"] {
        check_results!(
            ctx,
            view,
            "replication_filter",
            &[&[DfValue::Int(1)], &[DfValue::Int(2)], &[DfValue::Int(3)]],
        );
    }

    ctx.noria
        .view("t2")
        .await
        .expect_err("View should not exist, since viewed table does not exist");

    ctx.assert_table_missing("public", "t5").await;
    ctx.assert_table_exists("noria3", "t6").await;

    ctx.stop().await;
    client.stop().await;

    shutdown_tx.shutdown().await;

    Ok(())
}

async fn replication_all_schemas_inner(url: &str) -> ReadySetResult<()> {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await?;

    let cascade = if url.starts_with("postgresql") {
        "CASCADE"
    } else {
        ""
    };

    let query = format!(
        "
    DROP TABLE IF EXISTS t1 CASCADE; CREATE TABLE t1 (id int);
    DROP TABLE IF EXISTS t2 CASCADE; CREATE TABLE t2 (id int);
    DROP TABLE IF EXISTS t3 CASCADE; CREATE TABLE t3 (id int);
    DROP SCHEMA IF EXISTS noria2 {cascade}; CREATE SCHEMA noria2; CREATE TABLE noria2.t4 (id int);
    DROP SCHEMA IF EXISTS noria3 {cascade}; CREATE SCHEMA noria3;
    "
    );

    client.query(&query).await?;

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            replication_tables: Some("*.*".to_string()),
            ..Default::default()
        }),
    )
    .await?;

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    ctx.assert_table_exists("public", "t1").await;
    ctx.assert_table_exists("public", "t2").await;
    ctx.assert_table_exists("public", "t3").await;
    ctx.assert_table_exists("noria2", "t4").await;

    client
        .query(
            "
            CREATE TABLE noria2.t5 (id int);
            CREATE TABLE noria3.t6 (id int);
            INSERT INTO t1 VALUES (1),(2),(3);
            INSERT INTO t2 VALUES (1),(2),(3);
            INSERT INTO t3 VALUES (1),(2),(3);
            INSERT INTO noria2.t4 VALUES (1),(2),(3);
            ",
        )
        .await?;

    ctx.noria
        .extend_recipe(
            ChangeList::from_strings(
                vec!["CREATE VIEW public.t4_view AS SELECT * FROM noria2.t4;"],
                Dialect::DEFAULT_MYSQL,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    check_results!(
        ctx,
        "t4_view",
        "replication_filter",
        &[&[DfValue::Int(1)], &[DfValue::Int(2)], &[DfValue::Int(3)]],
    );

    ctx.assert_table_missing("public", "t5").await;
    ctx.assert_table_exists("noria3", "t6").await;

    ctx.stop().await;
    client.stop().await;

    shutdown_tx.shutdown().await;

    Ok(())
}

/// Tests that on encountering an ALTER TABLE statement the replicator does a proper resnapshot that
/// results in the proper schema being present.
async fn resnapshot_inner(url: &str) -> ReadySetResult<()> {
    let mut client = DbConnection::connect(url).await?;
    client
        .query(
            "
            DROP TABLE IF EXISTS repl1 CASCADE;
            DROP TABLE IF EXISTS repl2 CASCADE;
            DROP VIEW IF EXISTS repl1_view;
            DROP VIEW IF EXISTS repl2_view;
            CREATE TABLE repl1 (
                id int,
                val varchar(255)
            );
            CREATE TABLE repl2 (
                id int,
                val varchar(255)
            );
            CREATE VIEW repl1_view AS SELECT * FROM repl1;
            CREATE VIEW repl2_view AS SELECT * FROM repl2;",
        )
        .await?;

    const ROWS: usize = 20;

    // Populate both tables
    for i in 0..ROWS {
        client
            .query(&format!("INSERT INTO repl1 VALUES ({i}, 'I am a teapot')"))
            .await?;
        client
            .query(&format!("INSERT INTO repl2 VALUES ({i}, 'I am a teapot')"))
            .await?;
    }

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None).await?;
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    // Initial snapshot is done
    let rs: Vec<_> = (0..ROWS)
        .map(|i| [DfValue::from(i as i32), DfValue::from("I am a teapot")])
        .collect();
    let rs: Vec<&[DfValue]> = rs.iter().map(|r| r.as_slice()).collect();
    check_results!(ctx, "repl1_view", "Resnapshot initial", rs.as_slice());
    check_results!(ctx, "repl2_view", "Resnapshot initial", rs.as_slice());

    // Issue a few ALTER TABLE statements
    client.query("ALTER TABLE repl2 ADD COLUMN x int").await?;
    client.query("ALTER TABLE repl2 ADD COLUMN y int").await?;
    // Add some more rows in between
    for i in 0..ROWS {
        client
            .query(&format!("INSERT INTO repl1 VALUES ({i}, 'I am a teapot')"))
            .await?;
        client
            .query(&format!(
                "INSERT INTO repl2 VALUES ({i}, 'I am a teapot', {i}, {i})"
            ))
            .await?;
    }
    client.query("ALTER TABLE repl2 DROP COLUMN x").await?;

    // Check everything adds up for both the altered and unaltered tables
    let rs: Vec<_> = (0..ROWS)
        .flat_map(|i| {
            [
                [DfValue::from(i as i32), DfValue::from("I am a teapot")],
                [DfValue::from(i as i32), DfValue::from("I am a teapot")],
            ]
        })
        .collect();
    let rs: Vec<&[DfValue]> = rs.iter().map(|r| r.as_slice()).collect();
    check_results!(ctx, "repl1_view", "Resnapshot repl1", rs.as_slice());

    // TODO(fran): In theory the view should have been recreated properly, and this step should be
    // redundant
    client.query("DROP VIEW repl2_view").await?;
    client
        .query("CREATE VIEW repl2_view AS SELECT * FROM repl2")
        .await?;

    let rs: Vec<_> = (0..ROWS)
        .flat_map(|i| {
            [
                [
                    DfValue::from(i as i32),
                    DfValue::from("I am a teapot"),
                    DfValue::None,
                ],
                [
                    DfValue::from(i as i32),
                    DfValue::from("I am a teapot"),
                    DfValue::from(i as i32),
                ],
            ]
        })
        .collect();
    let rs: Vec<&[DfValue]> = rs.iter().map(|r| r.as_slice()).collect();

    // TODO(REA-3284): There appears to be an issue causing a view to remain stale even after being
    // dropped and recreated, which causes the below call to `check_results()` to fail due to
    // `repl2_view` missing a column. This sleep prevents the issue from happening.
    tokio::time::sleep(Duration::from_secs(1)).await;
    check_results!(ctx, "repl2_view", "Resnapshot repl2", rs.as_slice());

    ctx.stop().await;

    client
        .query(
            "DROP TABLE IF EXISTS repl1 CASCADE;
             DROP TABLE IF EXISTS repl2 CASCADE;
             DROP VIEW IF EXISTS repl1_view;
             DROP VIEW IF EXISTS repl2_view;",
        )
        .await?;

    client.stop().await;

    shutdown_tx.shutdown().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_enum_replication() {
    readyset_tracing::init_test_logging();
    let url = &mysql_url();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query(
            "
            DROP TABLE IF EXISTS `enum_test` CASCADE;
            DROP VIEW IF EXISTS enum_test_view;
            CREATE TABLE `enum_test` (
                id int NOT NULL PRIMARY KEY,
                e enum('red', 'yellow', 'green')
            );
            CREATE VIEW enum_test_view AS SELECT * FROM `enum_test` ORDER BY id ASC",
        )
        .await
        .unwrap();

    // Allow invalid values for enums
    client.query("SET @@sql_mode := ''").await.unwrap();
    client
        .query(
            "
            INSERT INTO enum_test VALUES
                (0, 'green'),
                (1, 'yellow'),
                (2, 'purple')",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "enum_test_view",
        "Snapshot",
        &[
            &[DfValue::Int(0), DfValue::Int(3)],
            &[DfValue::Int(1), DfValue::Int(2)],
            &[DfValue::Int(2), DfValue::Int(0)],
        ],
    );

    // Repeat, but this time using binlog replication
    client
        .query(
            "
            INSERT INTO enum_test VALUES
                (3, 'red'),
                (4, 'yellow')",
        )
        .await
        .unwrap();

    check_results!(
        ctx,
        "enum_test_view",
        "Replication",
        &[
            &[DfValue::Int(0), DfValue::Int(3)],
            &[DfValue::Int(1), DfValue::Int(2)],
            &[DfValue::Int(2), DfValue::Int(0)],
            &[DfValue::Int(3), DfValue::Int(1)],
            &[DfValue::Int(4), DfValue::Int(2)],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql8_upstream)]
async fn mysql_binlog_transaction_compression() {
    readyset_tracing::init_test_logging();
    let url = &mysql_url();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query("SET binlog_transaction_compression = 'ON'")
        .await
        .unwrap();
    client
        .query(
            "
            DROP TABLE IF EXISTS `binlog_compression_test`;
            CREATE TABLE `binlog_compression_test` (
                id int NOT NULL PRIMARY KEY,
                val varchar(255)
            );
            INSERT INTO binlog_compression_test VALUES (1, 'I am a teapot');
            INSERT INTO binlog_compression_test VALUES (2, 'I am not a teapot');
            DROP TABLE IF EXISTS `binlog_compression_test2`;
            CREATE TABLE `binlog_compression_test2` (
                id int NOT NULL PRIMARY KEY,
                val varchar(255)
            );
            INSERT INTO binlog_compression_test2 VALUES (1, 'I am a teapot');
            INSERT INTO binlog_compression_test2 VALUES (2, 'I am not a teapot');
            ",
        )
        .await
        .unwrap();
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    client
        .query(
            "START TRANSACTION;
        INSERT INTO binlog_compression_test VALUES (3, 'I am a big teapot');
        INSERT INTO binlog_compression_test2 VALUES (3, 'I am a big teapot');
        DELETE FROM binlog_compression_test WHERE id = 2;
        DELETE FROM binlog_compression_test2 WHERE id = 2;
        UPDATE binlog_compression_test SET val = 'I am a small teapot' WHERE id = 1;
        UPDATE binlog_compression_test2 SET val = 'I am a small teapot' WHERE id = 1;
        INSERT INTO binlog_compression_test2 VALUES (4, 'I am a tiny teapot');
        INSERT INTO binlog_compression_test VALUES (4, 'I am a tiny teapot');


        COMMIT;",
        )
        .await
        .unwrap();
    check_results!(
        ctx,
        "binlog_compression_test",
        "mysql_binlog_transaction_compression",
        &[
            &[DfValue::Int(1), DfValue::Text("I am a small teapot".into())],
            &[DfValue::Int(3), DfValue::Text("I am a big teapot".into())],
            &[DfValue::Int(4), DfValue::Text("I am a tiny teapot".into())],
        ],
    );
    check_results!(
        ctx,
        "binlog_compression_test2",
        "mysql_binlog_transaction_compression2",
        &[
            &[DfValue::Int(1), DfValue::Text("I am a small teapot".into())],
            &[DfValue::Int(3), DfValue::Text("I am a big teapot".into())],
            &[DfValue::Int(4), DfValue::Text("I am a tiny teapot".into())],
        ],
    );

    client.stop().await;
    ctx.stop().await;
    shutdown_tx.shutdown().await;
}

async fn postgresql_ddl_replicate_drop_table_internal(url: &str) {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query("DROP TABLE IF EXISTS t1 CASCADE; CREATE TABLE t1 (id int);")
        .await
        .unwrap();
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    ctx.noria
        .table(Relation {
            schema: Some("public".into()),
            name: "t1".into(),
        })
        .await
        .unwrap();

    trace!("Dropping table");
    client.query("DROP TABLE t1 CASCADE;").await.unwrap();

    eventually! {
        let res = ctx
            .noria
            .table(Relation {
                schema: Some("public".into()),
                name: "t1".into(),
            })
            .await;
        matches!(
            res.err(),
            Some(ReadySetError::RpcFailed { source, .. })
                if matches!(&*source, ReadySetError::TableNotFound{
                    name,
                    schema: Some(schema)
                } if name == "t1" && schema == "public")
        )
    }

    shutdown_tx.shutdown().await;
}

async fn postgresql_ddl_replicate_create_table_internal(url: &str) {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query("DROP TABLE IF EXISTS t2 CASCADE")
        .await
        .unwrap();
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    trace!("Creating table");
    client.query("CREATE TABLE t2 (id int);").await.unwrap();

    eventually!(ctx
        .noria
        .table(Relation {
            schema: Some("public".into()),
            name: "t2".into(),
        })
        .await
        .is_ok());

    shutdown_tx.shutdown().await;
}

async fn postgresql_ddl_replicate_drop_view_internal(url: &str) {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS t2 CASCADE; CREATE TABLE t2 (id int);
             DROP VIEW IF EXISTS t2_view; CREATE VIEW t2_view AS SELECT * FROM t2;",
        )
        .await
        .unwrap();
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    ctx.noria
        .table(Relation {
            schema: Some("public".into()),
            name: "t2".into(),
        })
        .await
        .unwrap();

    // Create a cache that reads from the view to test the view exists
    let create_cache_change = Change::CreateCache(CreateCache {
        name: Some(Relation {
            schema: Some("public".into()),
            name: "t2_view_q".into(),
        }),
        statement: Box::new(
            parse_select(
                readyset_sql::Dialect::PostgreSQL,
                "SELECT * FROM public.t2_view;",
            )
            .unwrap(),
        ),
        always: false,
    });
    ctx.noria
        .extend_recipe(ChangeList::from_change(
            create_cache_change.clone(),
            Dialect::DEFAULT_POSTGRESQL,
        ))
        .await
        .unwrap();

    trace!("Dropping view");
    client.query("DROP VIEW t2_view;").await.unwrap();

    eventually! {
        let res = ctx
            .noria
            .extend_recipe(ChangeList::from_change(
                create_cache_change.clone(),
                Dialect::DEFAULT_POSTGRESQL,
            ))
            .await;

        res.err()
           .as_ref()
           .and_then(|e| e.table_not_found_cause())
           .iter()
           .any(|(table, _)| *table == "t2_view")
    };

    shutdown_tx.shutdown().await;
}

async fn postgresql_ddl_replicate_create_view_internal(url: &str) {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS t2 CASCADE; CREATE TABLE t2 (id int);
            DROP VIEW IF EXISTS t2_view",
        )
        .await
        .unwrap();
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    ctx.noria
        .table(Relation {
            schema: Some("public".into()),
            name: "t2".into(),
        })
        .await
        .unwrap();

    trace!("CREATING view");
    client
        .query("CREATE VIEW t2_view AS SELECT * FROM t2;")
        .await
        .unwrap();

    // Create a cache that reads from the view to test the view exists
    eventually!(ctx
        .noria
        .extend_recipe(ChangeList::from_change(
            Change::CreateCache(CreateCache {
                name: Some(Relation {
                    schema: Some("public".into()),
                    name: "t2_view_q".into()
                }),
                statement: Box::new(
                    parse_select(
                        readyset_sql::Dialect::PostgreSQL,
                        "SELECT * FROM public.t2_view;"
                    )
                    .unwrap(),
                ),
                always: true,
            }),
            Dialect::DEFAULT_POSTGRESQL
        ))
        .await
        .is_ok());

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn snapshot_telemetry_mysql() {
    snapshot_telemetry_inner(&mysql_url()).await
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn snapshot_telemetry_postgresql() {
    snapshot_telemetry_inner(&pgsql_url()).await
}

async fn snapshot_telemetry_inner(url: &String) {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();
    client.query(CREATE_SCHEMA).await.unwrap();
    client.query(POPULATE_SCHEMA).await.unwrap();

    let (sender, mut reporter) = TelemetryInitializer::test_init();
    let mut builder = Builder::for_tests();
    builder.set_telemetry_sender(sender);
    let (mut ctx, shutdown_tx) =
        TestHandle::start_noria_with_builder(url.to_string(), None, builder)
            .await
            .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(ctx, "noria_view", "Snapshot", SNAPSHOT_RESULT);

    reporter.run_timeout(Duration::from_millis(20)).await;

    assert_eq!(
        1,
        reporter
            .check_event(TelemetryEvent::SnapshotComplete)
            .await
            .len()
    );

    let schemas = reporter
        .check_event(TelemetryEvent::Schema)
        .await
        .into_iter()
        .map(|t| t.schema.expect("should be some"))
        .collect::<Vec<_>>();
    let schema_str = format!("{schemas:?}");

    // MySQL has 1 create table and 1 create view -- postgres has 2 of each. Just assert that we
    // see at least one of each create type:

    assert!(schema_str.contains("CREATE TABLE"));
    assert!(schema_str.contains("CREATE VIEW"));

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn replication_tables_updates_mysql() {
    applies_replication_table_updates_on_restart(&mysql_url()).await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn replication_tables_updates_postgresql() {
    applies_replication_table_updates_on_restart(&pgsql_url()).await;
}

async fn applies_replication_table_updates_on_restart(url: &str) {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();

    // NOTE: We'll need to change this when we support domains; unfortunately failpoints don't work
    // before a controller starts
    client
        .query(
            "DROP TABLE IF EXISTS t1 CASCADE; CREATE TABLE t1(a INT);
            DROP TABLE IF EXISTS t2 CASCADE; CREATE TABLE t2 (a INT);",
        )
        .await
        .unwrap();

    let config = Config {
        replication_tables: Some(String::from("public.t1")),
        ..Default::default()
    };

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), Some(config))
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    let non_replicated_rels = ctx.noria.non_replicated_relations().await.unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t2".into(),
    };
    assert!(
        non_replicated_rels.contains(&NonReplicatedRelation::new(relation)),
        "non_replicated_rels = {non_replicated_rels:?}"
    );

    ctx.stop_repl().await;

    // Create a new test handle with different replication_tables config
    let config = Config {
        replication_tables: Some(String::from("public.t1, public.t2")),
        ..Default::default()
    };

    shutdown_tx.shutdown().await;

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), Some(config))
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    let non_replicated_rels = ctx.noria.non_replicated_relations().await.unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t2".into(),
    };
    assert!(
        !non_replicated_rels.contains(&NonReplicatedRelation::new(relation)),
        "non_replicated_rels = {non_replicated_rels:?}"
    );

    client
        .query(
            "DROP TABLE IF EXISTS t1 CASCADE;
            DROP TABLE IF EXISTS t2 CASCADE;",
        )
        .await
        .unwrap();

    client.stop().await;
    ctx.stop().await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_replicate_copy_from() {
    // Replication of data inserted via COPY is tricky due to it triggering the creation of
    // multiple replication events with the same LSN. This test was written to reproduce the
    // failure described in ENG-2100, where we could end up dropping rows due to multiple batches
    // of events ending with the same LSN and being erroneously skipped.
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS copy_from_t CASCADE;
             CREATE TABLE copy_from_t(v varchar(16));
             ALTER TABLE copy_from_t REPLICA IDENTITY FULL;
             CREATE VIEW copy_from_v AS SELECT v FROM copy_from_t;
             INSERT INTO copy_from_t VALUES ('snapshot check')",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "copy_from_v",
        "Snapshot",
        &[&[DfValue::from("snapshot check")]],
    );

    client
        .query(
            "DELETE FROM copy_from_t;
             COPY copy_from_t FROM PROGRAM 'seq 0 299';",
        )
        .await
        .unwrap();

    let expected_vals = (0..300)
        .map(|i| vec![DfValue::from(i.to_string())])
        .sorted()
        .collect::<Vec<_>>();
    let expected_slices = expected_vals
        .iter()
        .map(|v| v.as_slice())
        .collect::<Vec<_>>();
    check_results!(
        ctx,
        "copy_from_v",
        "Replication",
        expected_slices.as_slice()
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_non_base_offsets() {
    // This test reproduces the panic described in ENG-2204.
    // It is not clear to my why a JOIN is necessary to reproduce the problem, but a view that
    // queries from a single table does not seem to cause the same issue, so this was the minimal
    // failing case I was able to come up with.
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS t1 CASCADE;
             DROP TABLE IF EXISTS t2 CASCADE;
             CREATE TABLE t1(i integer);
             CREATE TABLE t2(j integer);
             CREATE OR REPLACE VIEW v1 AS SELECT i FROM t1 JOIN t2 ON i = j;
             INSERT INTO t1 VALUES (0);
             INSERT INTO t2 VALUES (0);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(ctx, "v1", "Snapshot", &[&[0.into()]]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_orphaned_nodes() {
    // This test checks for the error described in ENG-2190.
    // The bug would be triggered when a view would fail to be added due to certain types of
    // errors, because we would still call `commit` on the migration after the error occurred,
    // resulting in nodes being created in the domain without being added to the controller.
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    // The important thing about creating RS_FAKE_TEST_FUN is that it's not a builtin function, so
    // we will get a ReadySetError::NoSuchFunction during lowering and view creation will fail.
    client
        .query(
            "CREATE OR REPLACE FUNCTION RS_FAKE_TEST_FUN(bigint) RETURNS bigint
                 AS 'select $1' LANGUAGE SQL;
             DROP TABLE IF EXISTS t1 CASCADE;
             CREATE TABLE t1(i INTEGER);
             CREATE OR REPLACE VIEW v1 AS
                SELECT RS_FAKE_TEST_FUN(subq.my_count) FROM
                    (SELECT count(*) AS my_count FROM t1 GROUP BY i) subq;
             CREATE OR REPLACE VIEW check_t1 AS SELECT * FROM t1;
             INSERT INTO t1 VALUES(99);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(ctx, "check_t1", "Snapshot", &[&[99.into()]]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_replicate_citext() {
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS citext_t CASCADE;\
             CREATE EXTENSION IF NOT EXISTS citext;
             CREATE TABLE citext_t (t citext); \
             CREATE VIEW citext_v AS SELECT t FROM citext_t;",
        )
        .await
        .unwrap();
    client
        .query("INSERT INTO citext_t (t) VALUES ('abc'), ('AbC')")
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "citext_v",
        "Snapshot",
        &[
            &[DfValue::from_str_and_collation("AbC", Collation::Citext)],
            &[DfValue::from_str_and_collation("abc", Collation::Citext)],
        ],
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_replicate_citext_array() {
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS citext_t CASCADE;\
             CREATE EXTENSION IF NOT EXISTS citext;
             CREATE TABLE citext_t (t citext[]); \
             CREATE VIEW citext_v AS SELECT t FROM citext_t;",
        )
        .await
        .unwrap();
    client
        .query("INSERT INTO citext_t (t) VALUES ('{abc,DeF}'), ('{AbC,def}')")
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "citext_v",
        "Snapshot",
        &[
            &[DfValue::from(vec![
                DfValue::from_str_and_collation("abc", Collation::Citext),
                DfValue::from_str_and_collation("DeF", Collation::Citext),
            ])],
            &[DfValue::from(vec![
                DfValue::from_str_and_collation("AbC", Collation::Citext),
                DfValue::from_str_and_collation("def", Collation::Citext),
            ])],
        ],
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_replicate_custom_type() {
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS enum_table CASCADE;
             DROP TYPE IF EXISTS custom_enum;
             CREATE TYPE custom_enum AS ENUM ('a', 'b');
             CREATE TABLE enum_table (x custom_enum, xs custom_enum[]);
             CREATE VIEW enum_table_v AS SELECT x, xs FROM enum_table;",
        )
        .await
        .unwrap();

    client
        .query("INSERT INTO enum_table (x, xs) VALUES ('a', '{a, b}'), ('b', '{b, a}')")
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "enum_table_v",
        "Snapshot",
        &[
            &[
                DfValue::from(1),
                DfValue::from(vec![DfValue::from(1), DfValue::from(2)]),
            ],
            &[
                DfValue::from(2),
                DfValue::from(vec![DfValue::from(2), DfValue::from(1)]),
            ],
        ],
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_replicate_truncate() {
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();
    client
        .query(
            "DROP TABLE IF EXISTS t CASCADE;
             CREATE TABLE t (x int PRIMARY KEY);
             CREATE VIEW v AS SELECT x FROM t;
             INSERT INTO t (x) values (1), (2), (3);

             DROP TABLE IF EXISTS t2 CASCADE;
             CREATE TABLE t2 (y int PRIMARY KEY);
             CREATE VIEW v2 AS SELECT y FROM t2;
             INSERT INTO t2 (y) values (1), (2), (3);
            ",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "v",
        "pre-truncate",
        &[
            &[DfValue::from(1)],
            &[DfValue::from(2)],
            &[DfValue::from(3)],
        ],
    );

    check_results!(
        ctx,
        "v2",
        "pre-truncate",
        &[
            &[DfValue::from(1)],
            &[DfValue::from(2)],
            &[DfValue::from(3)],
        ],
    );

    client.query("TRUNCATE t, t2").await.unwrap();

    check_results!(ctx, "v", "post-truncate", &[]);
    check_results!(ctx, "v2", "post-truncate", &[]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_drop_nonexistent_replication_slot() {
    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let tls_connector = postgres_native_tls::MakeTlsConnector::new(connector);
    let (client, conn) = tokio_postgres::Config::from_str(&pgsql_url())
        .unwrap()
        .set_replication_database()
        .connect(tls_connector)
        .await
        .unwrap();

    let _conn = tokio::spawn(async move { conn.await.unwrap() });

    // This slot shouldn't exist, and should generate a corresponding "error", which we will usually
    // want to instead consider good enough from replication's point of view
    let slot_name = "doesnotexist";
    let res = client
        .simple_query(&format!("DROP_REPLICATION_SLOT {slot_name}"))
        .await;
    assert!(error_is_slot_not_found(&res.unwrap_err().into(), slot_name));

    // Different type of error shouldn't pass the check
    let slot_name = "invalid syntax";
    let res = client
        .simple_query(&format!("DROP_REPLICATION_SLOT {slot_name}"))
        .await;
    assert!(!error_is_slot_not_found(
        &res.unwrap_err().into(),
        slot_name
    ));
}

/// Given a table, check that it has an associated TOAST table
async fn postgresql_is_toasty(client: &tokio_postgres::client::Client, table: &str) -> bool {
    let res = client
        .query(
            "SELECT pg_table_size(c.reltoastrelid) AS toast_size
         FROM pg_catalog.pg_class c
         WHERE c.relname = $1 AND c.reltoastrelid > 0",
            &[&table],
        )
        .await
        .unwrap();
    res.len() == 1
}

/// An UPDATE to a TOAST-containing row, where one or more TOAST values are unmodified, should
/// replicate correctly.
/// Case 1: The table is unkeyed.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_toast_update_unkeyed() {
    readyset_tracing::init_test_logging();

    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let tls_connector = postgres_native_tls::MakeTlsConnector::new(connector);
    let url = pgsql_url();
    let (client, conn) = tokio_postgres::Config::from_str(&url)
        .unwrap()
        .connect(tls_connector)
        .await
        .unwrap();
    let _conn = tokio::spawn(async move { conn.await.unwrap() });

    // Make the TOAST random so it doesn't get compressed below the TOAST threshold
    let toast = rand::rngs::StdRng::seed_from_u64(0)
        .sample_iter(&Alphanumeric)
        .take(9001)
        .map(char::from)
        .collect::<String>();

    // Create a TOAST-able table (one with potentially large columns)
    // Set REPLICA IDENTITY FULL to allow updates to an unkeyed table
    // Create a view so we can check it in ReadySet later
    // Insert some TOAST
    client
        .simple_query(&format!(
            "DROP TABLE IF EXISTS t CASCADE;
             CREATE TABLE t (col1 INT, col2 TEXT);
             ALTER TABLE t REPLICA IDENTITY FULL;
             CREATE VIEW v AS SELECT * FROM t;
             INSERT INTO t VALUES (0, '{}');",
            &toast
        ))
        .await
        .unwrap();

    // Check that the table contains TOAST
    assert!(postgresql_is_toasty(&client, "t").await);

    // Snapshot the table
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Update the row, leaving the TOAST unchanged
    client
        .simple_query("UPDATE t SET col1 = 1 where col1 = 0")
        .await
        .unwrap();

    // Check that ReadySet replicated the update
    check_results!(
        ctx,
        "v",
        "toast_update_unkeyed",
        &[&[DfValue::from(1), DfValue::from(toast)]],
    );

    shutdown_tx.shutdown().await;
}

/// An UPDATE to a TOAST-containing row, where one or more TOAST values are unmodified, should
/// replicate correctly.
/// Case 2: The table is keyed, and one or more key columns is modified.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_toast_update_key() {
    readyset_tracing::init_test_logging();

    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let tls_connector = postgres_native_tls::MakeTlsConnector::new(connector);
    let url = pgsql_url();
    let (client, conn) = tokio_postgres::Config::from_str(&url)
        .unwrap()
        .connect(tls_connector)
        .await
        .unwrap();
    let _conn = tokio::spawn(async move { conn.await.unwrap() });

    let toast = rand::rngs::StdRng::seed_from_u64(0)
        .sample_iter(&Alphanumeric)
        .take(9001)
        .map(char::from)
        .collect::<String>()
        .into();

    // Create a TOAST-able table (one with potentially large columns)
    // Create a view so we can check it in ReadySet later
    // Insert some TOAST
    client
        .simple_query(&format!(
            "DROP TABLE IF EXISTS t CASCADE;
             CREATE TABLE t (col1 INT PRIMARY KEY, col2 TEXT);
             CREATE VIEW v AS SELECT * FROM t;
             INSERT INTO t VALUES (0, '{toast}');"
        ))
        .await
        .unwrap();

    // Check that the table contains TOAST
    assert!(postgresql_is_toasty(&client, "t").await);

    // Snapshot the table
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Update the row, leaving the TOAST unchanged
    client
        .simple_query("UPDATE t SET col1 = 1 where col1 = 0")
        .await
        .unwrap();

    // Check that ReadySet replicated the update
    check_results!(ctx, "v", "toast_update_key", &[&[DfValue::from(1), toast]],);

    shutdown_tx.shutdown().await;
}

/// An UPDATE to a TOAST-containing row, where one or more TOAST values are unmodified, should
/// replicate correctly.
/// Case 3: The table is keyed, but no key columns are modified.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgresql_toast_update_not_key() {
    readyset_tracing::init_test_logging();

    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let tls_connector = postgres_native_tls::MakeTlsConnector::new(connector);
    let url = pgsql_url();
    let (client, conn) = tokio_postgres::Config::from_str(&url)
        .unwrap()
        .connect(tls_connector)
        .await
        .unwrap();
    let _conn = tokio::spawn(async move { conn.await.unwrap() });

    let toast = rand::rngs::StdRng::seed_from_u64(0)
        .sample_iter(&Alphanumeric)
        .take(9001)
        .map(char::from)
        .collect::<String>();

    // Create a TOAST-able table (one with potentially large columns)
    // Create a view so we can check it in ReadySet later
    // Insert some TOAST
    client
        .simple_query(&format!(
            "DROP TABLE IF EXISTS t CASCADE;
             CREATE TABLE t (col1 INT PRIMARY KEY, col2 INT, col3 TEXT);
             CREATE VIEW v AS SELECT * FROM t;
             INSERT INTO t VALUES (0, 0, '{toast}');"
        ))
        .await
        .unwrap();

    // Check that the table contains TOAST
    assert!(postgresql_is_toasty(&client, "t").await);

    // Snapshot the table
    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Update the row, leaving the TOAST unchanged
    // Changing col2 here because its not the key
    client
        .simple_query("UPDATE t SET col2 = 1 where col2 = 0")
        .await
        .unwrap();

    // Check that ReadySet replicated the update
    check_results!(
        ctx,
        "v",
        "toast_update_not_key",
        &[&[DfValue::from(0), DfValue::from(1), DfValue::from(toast)]],
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_unsupported() {
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    // NOTE: We'll need to change this when we support domains; unfortunately failpoints don't work
    // before a controller starts
    client
        .query(
            "DROP DOMAIN IF EXISTS x CASCADE; CREATE DOMAIN x AS INTEGER;
            DROP TABLE IF EXISTS t2 CASCADE;
            DROP TABLE IF EXISTS t CASCADE; CREATE TABLE t (x x);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    let non_replicated_rels = ctx.noria.non_replicated_relations().await.unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t".into(),
    };
    assert!(non_replicated_rels.contains(&NonReplicatedRelation::new(relation)));

    // Replicate a new unsupported table
    client.query("CREATE TABLE t2 (x x);").await.unwrap();

    eventually! {
        let non_replicated_rels = ctx.noria.non_replicated_relations().await.unwrap();
        let relation = Relation{schema: Some("public".into()),
            name: "t2".into() };
        non_replicated_rels.contains(&NonReplicatedRelation::new(relation)

        )
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn pgsql_delete_from_table_without_pk() {
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS t2 CASCADE;
             DROP TABLE IF EXISTS t CASCADE; CREATE TABLE t (x int);
             INSERT INTO t (x) VALUES (1), (2);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "t",
        "pgsql_delete_from_table_without_pk",
        &[&[DfValue::from(1)], &[DfValue::from(2)]],
    );

    client.query("DELETE FROM t WHERE x = 1").await.unwrap();

    check_results!(
        ctx,
        "t",
        "pgsql_delete_from_table_without_pk",
        &[&[DfValue::from(2)]],
    );

    // Also check newly replicated tables

    client
        .query(
            "CREATE TABLE t2 (y int);
             INSERT INTO t2 (y) VALUES (1), (2);",
        )
        .await
        .unwrap();

    check_results!(
        ctx,
        "t2",
        "pgsql_delete_from_table_without_pk",
        &[&[DfValue::from(1)], &[DfValue::from(2)]],
    );

    client.query("DELETE FROM t2 WHERE y = 1").await.unwrap();

    check_results!(
        ctx,
        "t2",
        "pgsql_delete_from_table_without_pk",
        &[&[DfValue::from(2)]],
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, postgres_upstream)]
async fn pgsql_dont_replicate_partitioned_table() {
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS t CASCADE;
             DROP TABLE IF EXISTS t_true CASCADE;
             DROP TABLE IF EXISTS t_false CASCADE;

             CREATE TABLE t (key bool not null, val int) PARTITION BY LIST (key);
             CREATE TABLE t_true PARTITION OF t FOR VALUES IN (true);
             CREATE TABLE t_false PARTITION OF t FOR VALUES IN (false);

             INSERT INTO t (key, val) VALUES
             (true, 1),
             (true, 2),
             (false, 10),
             (false, 20);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    ctx.noria.table("t").await.unwrap_err();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t".into(),
    };

    assert!(ctx
        .noria
        .non_replicated_relations()
        .await
        .unwrap()
        .contains(&NonReplicatedRelation::new(relation.clone())));

    ctx.noria
        .table(Relation {
            schema: Some("public".into()),
            name: "t_true".into(),
        })
        .await
        .unwrap();
    ctx.noria
        .table(Relation {
            schema: Some("public".into()),
            name: "t_false".into(),
        })
        .await
        .unwrap();

    check_results!(
        ctx,
        "t_true",
        "pgsql_dont_replicate_partitioned_table",
        &[
            &[DfValue::from(true), DfValue::from(1)],
            &[DfValue::from(true), DfValue::from(2)],
        ],
    );
    check_results!(
        ctx,
        "t_false",
        "pgsql_dont_replicate_partitioned_table",
        &[
            &[DfValue::from(false), DfValue::from(10)],
            &[DfValue::from(false), DfValue::from(20)],
        ],
    );

    client
        .query("CREATE TABLE t2 (key int, val int) PARTITION BY RANGE (key)")
        .await
        .unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t2".into(),
    };
    eventually! {
        ctx
            .noria
            .non_replicated_relations()
            .await
            .unwrap()
            .contains(&NonReplicatedRelation::new(relation.clone()))
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, mysql_upstream)]
async fn mysql_dont_replicate_unsupported_storage_engine() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS t CASCADE;
             CREATE TABLE t (x int) ENGINE=MEMORY;",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    ctx.noria.table("t").await.unwrap_err();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t".into(),
    };

    assert!(ctx
        .noria
        .non_replicated_relations()
        .await
        .unwrap()
        .contains(&NonReplicatedRelation::new(relation.clone())));

    client
        .query(
            "DROP TABLE IF EXISTS t2 CASCADE;
            CREATE TABLE t2 (y int) ENGINE=MEMORY",
        )
        .await
        .unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "t2".into(),
    };
    eventually! {
        let nrr = ctx
            .noria
            .non_replicated_relations()
            .await
            .unwrap();
        debug!(?nrr);
        nrr.contains(&NonReplicatedRelation::new(relation.clone()))
    }

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_dont_enforce_fk_replication() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS t_parent CASCADE;
             DROP TABLE IF EXISTS t_child CASCADE;
             DROP TABLE IF EXISTS t_child2 CASCADE;
             CREATE TABLE t_parent (x INT PRIMARY KEY);
             CREATE TABLE t_child (y INT, FOREIGN KEY (y) REFERENCES t_parent(x));",
        )
        .await
        .unwrap();

    let config = Config {
        replication_tables: Some(String::from("public.t_child, public.t_child2")),
        ..Default::default()
    };

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), Some(config))
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    let t_parent = Relation {
        schema: Some("public".into()),
        name: "t_parent".into(),
    };

    let t_child = Relation {
        schema: Some("public".into()),
        name: "t_child".into(),
    };
    assert!(ctx
        .noria
        .non_replicated_relations()
        .await
        .unwrap()
        .contains(&NonReplicatedRelation::new(t_parent.clone())));

    assert!(ctx.noria.tables().await.unwrap().contains_key(&t_child));

    client
        .query("CREATE TABLE t_child2 (y INT, FOREIGN KEY (y) REFERENCES t_parent(x));")
        .await
        .unwrap();
    let t_child2 = Relation {
        schema: Some("public".into()),
        name: "t_child2".into(),
    };
    eventually! {
        ctx.noria.tables().await.unwrap().contains_key(&t_child2)
    }
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_generated_columns() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    // Test Generated Columns during Snapshot
    client
        .query(
            "DROP TABLE IF EXISTS t_generated CASCADE;
             CREATE TABLE t_generated (x INT PRIMARY KEY, col1 INT AS (x + 1) VIRTUAL, col2 INT AS (x + 2), col3 INT AS (x + 3) STORED);
             INSERT INTO t_generated (x) VALUES (1);
             INSERT INTO t_generated (x) VALUES (2);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "t_generated",
        "test_snapshot",
        &[
            &[
                DfValue::from(1),
                DfValue::from(2),
                DfValue::from(3),
                DfValue::from(4),
            ],
            &[
                DfValue::from(2),
                DfValue::from(3),
                DfValue::from(4),
                DfValue::from(5),
            ],
        ],
    );

    // Test Generated Columns during Replication including updating a record and checking the
    // generated columns reflect the update and deleting a record.
    client
        .query(
            "INSERT INTO t_generated (x) VALUES (10);
             UPDATE t_generated SET x = 5 WHERE x = 1;
             DELETE FROM t_generated WHERE x = 2;",
        )
        .await
        .unwrap();
    check_results!(
        ctx,
        "t_generated",
        "test_snapshot",
        &[
            &[
                DfValue::from(5),
                DfValue::from(6),
                DfValue::from(7),
                DfValue::from(8),
            ],
            &[
                DfValue::from(10),
                DfValue::from(11),
                DfValue::from(12),
                DfValue::from(13),
            ],
        ],
    );
    shutdown_tx.shutdown().await;
}

/// Tests that on encountering an ALTER TABLE statement the replicator does not resnapshot tables
/// with foreign keys.
async fn fk_resnapshot_inner(url: &str) -> ReadySetResult<()> {
    readyset_tracing::init_test_logging();
    let mut client = DbConnection::connect(url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS fk_table1 CASCADE;
             CREATE TABLE fk_table1 (x INT PRIMARY KEY, p_id INT, CONSTRAINT fk_table1_id FOREIGN KEY (p_id) REFERENCES fk_table1 (x));
             INSERT INTO fk_table1 (x, p_id) VALUES (1, 1);
             CREATE TABLE dummy (ID INT PRIMARY KEY);
             INSERT INTO dummy (ID) VALUES (1);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "fk_table1",
        "Snapshot1",
        &[&[DfValue::Int(1), DfValue::Int(1)]],
    );
    let fk_relation = Relation {
        schema: Some("public".into()),
        name: "fk_table1".into(),
    };
    let tables = ctx.noria.tables().await.unwrap();
    let table_index_first_snapshot = tables.get(&fk_relation).unwrap();

    // triggers resnapshot
    client
        .query(
            "ALTER TABLE dummy ADD COLUMN a INT;
            INSERT INTO fk_table1 (x, p_id) VALUES (2, 1);",
        )
        .await
        .unwrap();

    check_results!(
        ctx,
        "fk_table1",
        "Snapshot2",
        &[
            &[DfValue::Int(1), DfValue::Int(1)],
            &[DfValue::Int(2), DfValue::Int(1)],
        ],
    );

    let tables = ctx.noria.tables().await.unwrap();
    let table_index_second_snapshot = tables.get(&fk_relation).unwrap();

    // Ensure we did not resnapshot the table by checking the domain index
    assert_eq!(table_index_first_snapshot, table_index_second_snapshot);

    shutdown_tx.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_fk_tables_resnapshot() {
    fk_resnapshot_inner(&mysql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn postgres_fk_tables_resnapshot() {
    fk_resnapshot_inner(&pgsql_url()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_handle_dml_in_statement_events() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS stmt_table CASCADE;
             CREATE TABLE stmt_table (x INT PRIMARY KEY);
             INSERT INTO stmt_table (x) VALUES (1);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(ctx, "stmt_table", "Snapshot1", &[&[DfValue::Int(1)]]);

    // Ensure that:
    // 1. The base table exists
    // 2. There is a cache created on previous step
    let tables = ctx.noria.tables().await.unwrap();
    let caches = ctx.noria.views().await.unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "stmt_table".into(),
    };
    assert!(tables.contains_key(&relation));
    assert_eq!(caches.len(), 1);

    client
        .query("SET binlog_format=STATEMENT; INSERT INTO stmt_table (x) VALUES (2);")
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    // Ensure that:
    // 1. The base table has been dropped
    // 2. The table is not replicated
    // 3. The cache has been dropped
    let tables = ctx.noria.tables().await.unwrap();
    let non_replicated_tables = ctx.noria.non_replicated_relations().await.unwrap();
    let caches = ctx.noria.views().await.unwrap();
    assert!(tables.is_empty());
    assert!(non_replicated_tables.contains(&NonReplicatedRelation::new(relation)));
    assert_eq!(caches.len(), 0);
    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_replicate_json_field() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS j_table;
            CREATE TABLE j_table (id INT PRIMARY KEY, data JSON, c CHAR(1));
            INSERT INTO j_table (id, data, c) VALUES (1, '{\"age\": 30, \"car\": [\"Ford\", \"BMW\", \"Fiat\"], \"name\": \"John\"}', 'A');
            INSERT INTO j_table (id, data, c) VALUES (2, '{\"car\": [\"Ford\", \"BMW\", \"Fiat\"], \"name\": \"John\", \"age\":30}', 'A');
            INSERT INTO j_table (id, data, c) VALUES (3, '{\"amount\": {\"amount\": \"0.0\", \"currency\": \"USD\"}, \"is_custom\": false, \"description\": \"My Description\", \"amount_formatted\": \"$0.00\"}', 'A');
            INSERT INTO j_table (id, data, c) VALUES (4, NULL, 'A');",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();
    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Check that the row is replicated correctly
    check_results!(ctx, "j_table",
    "Snapshot1",
    &[
        &[
            DfValue::Int(1),
            DfValue::Text(
                "{\"age\":30,\"car\":[\"Ford\",\"BMW\",\"Fiat\"],\"name\":\"John\"}"
                    .into(),
            ),
            DfValue::Text("A".into()),
        ],
        &[
            DfValue::Int(2),
            DfValue::Text(
                "{\"age\":30,\"car\":[\"Ford\",\"BMW\",\"Fiat\"],\"name\":\"John\"}"
                    .into(),
            ),
            DfValue::Text("A".into()),
        ],
        &[
            DfValue::Int(3),
            DfValue::Text(
                "{\"amount\":{\"amount\":\"0.0\",\"currency\":\"USD\"},\"amount_formatted\":\"$0.00\",\"description\":\"My Description\",\"is_custom\":false}"
                    .into(),
            ),
            DfValue::Text("A".into()),
        ],
        &[DfValue::Int(4), DfValue::None, DfValue::Text("A".into())],
    ],);

    // Update the JSON data
    client
        .query("UPDATE j_table SET c = 'B' WHERE id IN (1, 2, 3, 4);")
        .await
        .unwrap();

    // Check that the update is replicated correctly
    check_results!(ctx, "j_table",
    "Replication",
    &[
        &[
            DfValue::Int(1),
            DfValue::Text(
                "{\"age\":30,\"car\":[\"Ford\",\"BMW\",\"Fiat\"],\"name\":\"John\"}"
                    .into(),
            ),
            DfValue::Text("B".into()),
        ],
        &[
            DfValue::Int(2),
            DfValue::Text(
                "{\"age\":30,\"car\":[\"Ford\",\"BMW\",\"Fiat\"],\"name\":\"John\"}"
                    .into(),
            ),
            DfValue::Text("B".into()),
        ],
        &[
            DfValue::Int(3),
            DfValue::Text(
                "{\"amount\":{\"amount\":\"0.0\",\"currency\":\"USD\"},\"amount_formatted\":\"$0.00\",\"description\":\"My Description\",\"is_custom\":false}"
                    .into(),
            ),
            DfValue::Text("B".into()),
        ],
        &[DfValue::Int(4), DfValue::None, DfValue::Text("B".into())],
    ],);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn alter_readyset_add_table_replication_tables() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    let query = "
    DROP TABLE IF EXISTS filter_t1; CREATE TABLE filter_t1 (id int);
    DROP TABLE IF EXISTS filter_t2; CREATE TABLE filter_t2 (id int);
    DROP SCHEMA IF EXISTS noria2; CREATE SCHEMA noria2; CREATE TABLE noria2.filter_t3 (id int);
    CREATE TABLE noria2.filter_t4 (id int);
    "
    .to_string();

    client.query(&query).await.unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            replication_tables: Some("public.filter_t1,noria2.filter_t3".to_string()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    ctx.assert_table_exists("public", "filter_t1").await;
    ctx.assert_table_exists("noria2", "filter_t3").await;
    ctx.assert_table_missing("public", "filter_t2").await;
    ctx.assert_table_missing("noria2", "filter_t4").await;

    let table_2 = Relation {
        schema: Some("public".into()),
        name: "filter_t2".into(),
    };
    let table_4 = Relation {
        schema: Some("noria2".into()),
        name: "filter_t4".into(),
    };
    ctx.replicator_tx
        .as_ref()
        .unwrap()
        .send(ReplicatorMessage::AddTables {
            tables: vec![table_2.clone(), table_4.clone()],
        })
        .unwrap();

    sleep(Duration::from_secs(4)).await;

    eventually! {
        ctx.noria.tables().await.unwrap().contains_key(
            &table_2
        ) && ctx.noria.tables().await.unwrap().contains_key(
            &table_4
        )
    }
    let query = "INSERT INTO filter_t1 (id) VALUES (1);
        INSERT INTO filter_t2 (id) VALUES (2);"
        .to_string();
    client.query(&query).await.unwrap();
    check_results!(ctx, "filter_t1", "filter_t1", &[&[DfValue::from(1)]]);
    check_results!(ctx, "filter_t2", "filter_t2", &[&[DfValue::from(2)]]);

    ctx.stop().await;
    client.stop().await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn alter_readyset_add_table_replication_tables_ignore() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    let query = "DROP TABLE IF EXISTS filter_t1; CREATE TABLE filter_t1 (id int);
    DROP TABLE IF EXISTS filter_t2; CREATE TABLE filter_t2 (id int);
    DROP SCHEMA IF EXISTS noria2; CREATE SCHEMA noria2; CREATE TABLE noria2.filter_t3 (id int);
    CREATE TABLE noria2.filter_t4 (id int);
    "
    .to_string();

    client.query(&query).await.unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(
        url.to_string(),
        Some(Config {
            replication_tables_ignore: Some("public.*,noria2.filter_t3".to_string()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();
    ctx.assert_table_missing("public", "filter_t1").await;
    ctx.assert_table_missing("public", "filter_t2").await;
    ctx.assert_table_missing("noria2", "filter_t3").await;
    ctx.assert_table_exists("noria2", "filter_t4").await;

    let table_1 = Relation {
        schema: Some("public".into()),
        name: "filter_t1".into(),
    };
    let table_2 = Relation {
        schema: Some("public".into()),
        name: "filter_t2".into(),
    };
    let table_3 = Relation {
        schema: Some("noria2".into()),
        name: "filter_t3".into(),
    };
    ctx.replicator_tx
        .as_ref()
        .unwrap()
        .send(ReplicatorMessage::AddTables {
            tables: vec![table_1.clone(), table_2.clone(), table_3.clone()],
        })
        .unwrap();

    sleep(Duration::from_secs(4)).await;

    eventually! {
        ctx.noria.tables().await.unwrap().contains_key(
            &table_1
        ) && ctx.noria.tables().await.unwrap().contains_key(
            &table_2
        ) && ctx.noria.tables().await.unwrap().contains_key(
            &table_3
        )
    }
    let query = "INSERT INTO filter_t1 (id) VALUES (1);
        INSERT INTO filter_t2 (id) VALUES (2);"
        .to_string();
    client.query(&query).await.unwrap();
    check_results!(ctx, "filter_t1", "filter_t1", &[&[DfValue::from(1)]]);
    check_results!(ctx, "filter_t2", "filter_t2", &[&[DfValue::from(2)]]);

    ctx.stop().await;
    client.stop().await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_minimal_row_based_replication() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client.query("DROP TABLE IF EXISTS t; CREATE TABLE no_pk (x int, b char(1) default 'a', c int default null, d int default 1);").await.unwrap();
    client.query("DROP TABLE IF EXISTS t; CREATE TABLE no_pk_with_uk (x int, b char(1) default 'a', c int default null, d int default 1, unique key (b));").await.unwrap();
    client.query("DROP TABLE IF EXISTS t; CREATE TABLE pk (x int primary key, b char(1) default 'a', c int default null, d int default 1);").await.unwrap();
    client
        .query("SET binlog_row_image = minimal;")
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    // Insert some data
    client
        .query("INSERT INTO no_pk (x) VALUES (1);")
        .await
        .unwrap();
    client
        .query("INSERT INTO no_pk_with_uk (x) VALUES (1);")
        .await
        .unwrap();
    client
        .query("INSERT INTO pk (x) VALUES (1);")
        .await
        .unwrap();

    // Check that the data is replicated
    let row = [
        DfValue::Int(1),
        DfValue::from_str_and_collation("a", Collation::Citext),
        DfValue::None,
        DfValue::Int(1),
    ];
    check_results!(ctx, "no_pk", "no_pk_insert", &[&row]);
    check_results!(ctx, "no_pk_with_uk", "no_pk_with_uk_insert", &[&row]);
    check_results!(ctx, "pk", "pk_insert", &[&row]);

    // update some of the columns
    client
        .query("UPDATE no_pk SET b = 'b' where x = 1;")
        .await
        .unwrap();
    client
        .query("UPDATE no_pk_with_uk SET b = 'b' where x = 1;")
        .await
        .unwrap();
    client
        .query("UPDATE pk SET b = 'b' where x = 1;")
        .await
        .unwrap();

    // Check that the data is replicated
    let row = [
        DfValue::Int(1),
        DfValue::from_str_and_collation("b", Collation::Citext),
        DfValue::None,
        DfValue::Int(1),
    ];
    check_results!(ctx, "no_pk", "no_pk_update", &[&row]);
    check_results!(ctx, "no_pk_with_uk", "no_pk_with_uk_update", &[&row]);
    check_results!(ctx, "pk", "pk_update", &[&row]);

    // delete the data
    client
        .query("DELETE FROM no_pk where x = 1;")
        .await
        .unwrap();
    client
        .query("DELETE FROM no_pk_with_uk where x = 1;")
        .await
        .unwrap();
    client.query("DELETE FROM pk where x = 1;").await.unwrap();

    // Check that the data is deleted
    check_results!(ctx, "no_pk", "no_pk_delete", &[]);
    check_results!(ctx, "no_pk_with_uk", "no_pk_with_uk_delete", &[]);
    check_results!(ctx, "pk", "pk_delete", &[]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_minimal_row_based_replication_empty_row() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query("DROP TABLE IF EXISTS mrbr_empty_row")
        .await
        .unwrap();
    client
        .query("CREATE TABLE mrbr_empty_row(b int default 1)")
        .await
        .unwrap();
    client
        .query("SET binlog_row_image = minimal;")
        .await
        .unwrap();
    client.query("SET sql_mode = '';").await.unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    client
        .query("INSERT INTO mrbr_empty_row () VALUES ();")
        .await
        .unwrap();

    check_results!(
        ctx,
        "mrbr_empty_row",
        "mrbr_empty_row_insert",
        &[&[DfValue::Int(1)]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_minimal_row_based_collation_and_signedness() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query("SET binlog_row_image = minimal;")
        .await
        .unwrap();
    client
        .query("DROP TABLE IF EXISTS mrbr_collation_and_signedness;")
        .await
        .unwrap();
    client.query("CREATE TABLE `mrbr_collation_and_signedness` (`col_1` int NOT NULL, `col_2` blob, `col_3` char(2) DEFAULT NULL, PRIMARY KEY (`col_1`));").await.unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    client
        .query("INSERT INTO `mrbr_collation_and_signedness` (`col_1`, `col_3`) VALUES (0, 'a');")
        .await
        .unwrap();

    check_results!(
        ctx,
        "mrbr_collation_and_signedness",
        "mrbr_collation_and_signedness_insert",
        &[&[
            DfValue::Int(0),
            DfValue::None,
            DfValue::from_str_and_collation("a ", Collation::Citext)
        ]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_minimal_row_based_char_padding() {
    // REA-5699
    // This test will create a table without PK. The insert will ommit column n, since we are using MRBR it will be deferred to Readyset to fill in the default value.
    // Then we will update the column and ensure that the update is replicated correctly. Because the table has no PK, this will be a full row delete followed by an insert.
    // If we did not chose the right default value, the delete will fail.
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query("SET binlog_row_image = minimal;")
        .await
        .unwrap();
    client.query("SET sql_mode = '';").await.unwrap();
    client
        .query("DROP TABLE IF EXISTS mrbr_char_padding;")
        .await
        .unwrap();
    client
        .query("CREATE TABLE `mrbr_char_padding` (ID INT, n CHAR(10) NOT NULL);")
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    client
        .query("INSERT INTO `mrbr_char_padding` (ID) VALUES (0);")
        .await
        .unwrap();
    client
        .query("UPDATE `mrbr_char_padding` SET n = 'a' WHERE ID = 0;")
        .await
        .unwrap();

    check_results!(
        ctx,
        "mrbr_char_padding",
        "mrbr_char_padding_insert",
        &[&[
            DfValue::Int(0),
            DfValue::from_str_and_collation("a         ", Collation::Citext),
        ]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_minimal_row_based_binary() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query("SET binlog_row_image = minimal;")
        .await
        .unwrap();
    client.query("SET sql_mode = '';").await.unwrap();
    client
        .query("DROP TABLE IF EXISTS mrbr_binary;")
        .await
        .unwrap();
    client
        .query(
            "CREATE TABLE mrbr_binary (id INT, b BINARY(10) NOT NULL, vb VARBINARY(10) NOT NULL);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    client
        .query("INSERT INTO mrbr_binary (id) VALUES (0);")
        .await
        .unwrap();

    check_results!(
        ctx,
        "mrbr_binary",
        "mrbr_binary_insert",
        &[&[
            DfValue::Int(0),
            DfValue::ByteArray(vec![0; 10].into()),
            DfValue::ByteArray(vec![0; 0].into())
        ]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn mysql_minimal_row_based_blob() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query("SET binlog_row_image = minimal;")
        .await
        .unwrap();
    client.query("SET sql_mode = '';").await.unwrap();
    client
        .query("DROP TABLE IF EXISTS mrbr_blob;")
        .await
        .unwrap();
    client
        .query("CREATE TABLE mrbr_blob (id INT, b BLOB NOT NULL);")
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    client
        .query("INSERT INTO mrbr_blob (id) VALUES (0);")
        .await
        .unwrap();

    check_results!(
        ctx,
        "mrbr_blob",
        "mrbr_blob_insert",
        &[&[DfValue::Int(0), DfValue::ByteArray(vec![].into())]]
    );

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, mysql_upstream)]
async fn alter_table_add_key_mysql() {
    readyset_tracing::init_test_logging();
    let url = mysql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS test_alter_table_add_key;
            DROP TABLE IF EXISTS test_alter_table_add_key2;
            DROP TABLE IF EXISTS test_alter_table_add_key_bogus;
        CREATE TABLE test_alter_table_add_key (id INT PRIMARY KEY, c INT NOT NULL, d VARCHAR(10));
        CREATE TABLE test_alter_table_add_key2 (id INT PRIMARY KEY);
        CREATE TABLE test_alter_table_add_key_bogus (id INT PRIMARY KEY);
        INSERT INTO test_alter_table_add_key (id, c, d) VALUES (1, 2, 'a');
        INSERT INTO test_alter_table_add_key2 (id) VALUES (2);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "test_alter_table_add_key",
        "Snapshot1",
        &[&[
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::from_str_and_collation("a", Collation::Citext)
        ]]
    );

    // Ensure that:
    // 1. The base table exists
    // 2. There is a cache created on previous step
    let tables = ctx.noria.tables().await.unwrap();
    let caches = ctx.noria.views().await.unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "test_alter_table_add_key".into(),
    };
    assert!(tables.contains_key(&relation));
    assert_eq!(caches.len(), 1);
    let table_id = tables.get(&relation).unwrap();
    let bogus_table = Relation {
        schema: Some("public".into()),
        name: "test_alter_table_add_key_bogus".into(),
    };
    let bogus_table_id = tables.get(&bogus_table).unwrap();

    let verify_tables_and_caches =
        async move |ctx: &mut TestHandle, expected_table_id: &NodeIndex| -> ReadySetResult<()> {
            let tables = ctx.noria.tables().await.unwrap();
            let caches = ctx.noria.views().await.unwrap();
            assert_eq!(tables.len(), 3, "tables.len() = {}", tables.len());
            assert_eq!(caches.len(), 1, "caches.len() = {}", caches.len());
            let table_id_after = tables.get(&relation).unwrap();
            assert_eq!(
                table_id_after, expected_table_id,
                "table_id_after = {:?}, expected_table_id = {:?}",
                table_id_after, expected_table_id
            );
            Ok(())
        };

    // Add a new key
    client
        .query("ALTER TABLE test_alter_table_add_key ADD KEY (c), ADD FOREIGN KEY (c) REFERENCES test_alter_table_add_key2 (id), ADD KEY (c), ADD FULLTEXT KEY (d);")
        .await
        .unwrap();

    // Wait in case of resnapshot for the snapshot to complete
    eventually!(verify_tables_and_caches(&mut ctx, table_id).await.is_ok());

    // trigger resnapshot

    ctx.replicator_tx
        .as_ref()
        .unwrap()
        .send(ReplicatorMessage::ResnapshotTable {
            table: bogus_table.clone(),
        })
        .unwrap();

    eventually!(
            let tables = ctx.noria.tables().await.unwrap();
            let new_bogus_table_id = tables.get(&bogus_table);
            new_bogus_table_id.is_some() && new_bogus_table_id.unwrap() != bogus_table_id
    );

    // Verify again after resnapshot
    verify_tables_and_caches(&mut ctx, table_id).await.unwrap();

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow, postgres_upstream)]
async fn alter_table_add_key_postgres() {
    readyset_tracing::init_test_logging();
    let url = pgsql_url();
    let mut client = DbConnection::connect(&url).await.unwrap();

    client
        .query(
            "DROP TABLE IF EXISTS test_alter_table_add_key;
            DROP TABLE IF EXISTS test_alter_table_add_key2;
            DROP TABLE IF EXISTS test_alter_table_add_key_bogus;
        CREATE TABLE test_alter_table_add_key (id INT PRIMARY KEY, c INT NOT NULL, d VARCHAR(10));
        CREATE TABLE test_alter_table_add_key2 (id INT PRIMARY KEY);
        CREATE TABLE test_alter_table_add_key_bogus (id INT PRIMARY KEY);
        INSERT INTO test_alter_table_add_key (id, c, d) VALUES (1, 2, 'a');
        INSERT INTO test_alter_table_add_key2 (id) VALUES (2);",
        )
        .await
        .unwrap();

    let (mut ctx, shutdown_tx) = TestHandle::start_noria(url.to_string(), None)
        .await
        .unwrap();

    ctx.controller_rx
        .as_mut()
        .unwrap()
        .snapshot_completed()
        .await
        .unwrap();

    check_results!(
        ctx,
        "test_alter_table_add_key",
        "Snapshot1",
        &[&[
            DfValue::Int(1),
            DfValue::Int(2),
            DfValue::from_str_and_collation("a", Collation::Citext)
        ]]
    );

    // Ensure that:
    // 1. The base table exists
    // 2. There is a cache created on previous step
    let tables = ctx.noria.tables().await.unwrap();
    let caches = ctx.noria.views().await.unwrap();
    let relation = Relation {
        schema: Some("public".into()),
        name: "test_alter_table_add_key".into(),
    };
    assert!(tables.contains_key(&relation));
    assert_eq!(caches.len(), 1);
    let table_id = tables.get(&relation).unwrap();
    let bogus_table = Relation {
        schema: Some("public".into()),
        name: "test_alter_table_add_key_bogus".into(),
    };
    let bogus_table_id = tables.get(&bogus_table).unwrap();

    let verify_tables_and_caches =
        async move |ctx: &mut TestHandle, expected_table_id: &NodeIndex| -> ReadySetResult<()> {
            let tables = ctx.noria.tables().await.unwrap();
            let caches = ctx.noria.views().await.unwrap();
            assert_eq!(tables.len(), 3, "tables.len() = {}", tables.len());
            assert_eq!(caches.len(), 1, "caches.len() = {}", caches.len());
            let table_id_after = tables.get(&relation).unwrap();
            assert_eq!(
                table_id_after, expected_table_id,
                "table_id_after = {:?}, expected_table_id = {:?}",
                table_id_after, expected_table_id
            );
            Ok(())
        };

    // Add a new key
    client
        .query("CREATE INDEX ON test_alter_table_add_key (c);")
        .await
        .unwrap();

    client
        .query("CREATE INDEX test_alter_table_add_key_c_idx2 ON test_alter_table_add_key (c);")
        .await
        .unwrap();

    client
        .query("CREATE INDEX ON test_alter_table_add_key (c);")
        .await
        .unwrap();
    client
        .query(
            "ALTER TABLE test_alter_table_add_key
  ADD CONSTRAINT myfk
  FOREIGN KEY (c) REFERENCES test_alter_table_add_key2 (id);",
        )
        .await
        .unwrap();

    eventually!(verify_tables_and_caches(&mut ctx, table_id).await.is_ok());

    // trigger resnapshot

    ctx.replicator_tx
        .as_ref()
        .unwrap()
        .send(ReplicatorMessage::ResnapshotTable {
            table: bogus_table.clone(),
        })
        .unwrap();

    eventually!(
            let tables = ctx.noria.tables().await.unwrap();
            let new_bogus_table_id = tables.get(&bogus_table);
            new_bogus_table_id.is_some() && new_bogus_table_id.unwrap() != bogus_table_id
    );

    // Verify again after resnapshot
    verify_tables_and_caches(&mut ctx, table_id).await.unwrap();

    shutdown_tx.shutdown().await;
}
