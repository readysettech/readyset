//! Compares Readyset with MySQL and Postgres. A test fills a table `m` with rows, caches a
//! statement of each list of expressions it's given, reads each through Readyset and from the
//! upstream, and fails with every expression whose type or values differ.

/// How many statements run at once.
const PARALLEL: usize = 16;

pub(crate) mod mysql {
    use futures::{StreamExt, stream};
    use mysql_async::consts::ColumnType;
    use mysql_async::prelude::Queryable;
    use mysql_async::{Conn, Error, Opts, Pool, Row, Value};
    use readyset_adapter::backend::MigrationMode;
    use readyset_client_metrics::QueryDestination;
    use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
    use readyset_client_test_helpers::{TestBuilder, derive_test_name, sleep};
    use readyset_sql_parsing::ParsingPreset;

    use super::PARALLEL;

    /// An expression's result: its type, its digits after the point, and its value in each row.
    #[derive(PartialEq)]
    struct Column {
        ty: ColumnType,
        decimals: u8,
        values: Vec<Value>,
    }

    impl Column {
        /// Drops the trailing zeros after the point of each decimal value.
        fn trim_zeros(&mut self) {
            for value in &mut self.values {
                if let Value::Bytes(bytes) = value
                    && let Ok(text) = std::str::from_utf8(bytes)
                    && text.contains('.')
                {
                    *value = Value::from(text.trim_end_matches('0').trim_end_matches('.'));
                }
            }
        }
    }

    impl std::fmt::Debug for Column {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let values: Vec<String> = self
                .values
                .iter()
                .map(|value| value.as_sql(false))
                .collect();
            write!(
                f,
                "{:?}({}) [{}]",
                self.ty,
                self.decimals,
                values.join(", ")
            )
        }
    }

    /// Returns the statement selecting the id and `exprs` from `from`.
    fn select(from: &str, exprs: &[String]) -> String {
        format!("SELECT id, {} FROM {from}", exprs.join(", "))
    }

    /// Reads `sql` and returns each column after the id, with the rows in id order, or the error.
    async fn read(conn: &mut Conn, sql: &str) -> Result<Vec<Column>, String> {
        let mut rows: Vec<Row> = match conn.exec(sql, ()).await {
            Ok(rows) => rows,
            Err(Error::Server(err)) => return Err(format!("error {}", err.code)),
            Err(err) => return Err(err.to_string()),
        };
        rows.sort_by_key(|row| row.get::<i32, _>(0));
        let columns = rows[0].columns_ref();
        Ok((1..columns.len())
            .map(|i| Column {
                ty: columns[i].column_type(),
                decimals: columns[i].decimals(),
                values: rows.iter().map(|row| row[i].clone()).collect(),
            })
            .collect())
    }

    /// Reads the statement selecting `exprs` through Readyset and from MySQL, and returns each
    /// expression with a different type or values, and whether Readyset sent the read to MySQL.
    async fn check(
        readyset: &mut Conn,
        upstream: &mut Conn,
        from: &str,
        exprs: Vec<String>,
    ) -> Vec<String> {
        let sql = select(from, &exprs);
        let want = read(upstream, &sql).await;
        let got = read(readyset, &sql).await;
        let (got, want) = match (got, want) {
            (Ok(got), Ok(want)) => (got, want),
            (got, want) if got == want => return Vec::new(),
            (got, want) => return vec![format!("{sql}: Readyset {got:?}, MySQL {want:?}")],
        };
        let mut mismatches = Vec::new();
        let destination = last_query_info(readyset).await.destination;
        if !matches!(destination, QueryDestination::Readyset(_)) {
            mismatches.push(format!("{sql}: Readyset sent it to MySQL"));
        }
        for ((expr, mut got), mut want) in exprs.iter().zip(got).zip(want) {
            // TODO(REA-7193): MySQL writes each value of a CASE with its own branch's digits after
            // the point, and Readyset with the CASE's, so the test compares a CASE's values as
            // numbers.
            if expr.starts_with("CASE") {
                got.trim_zeros();
                want.trim_zeros();
            }
            if got != want {
                mismatches.push(format!("{expr}: Readyset {got:?}, MySQL {want:?}"));
            }
        }
        mismatches
    }

    /// Fills the table `m` with `rows`, caches a statement selecting each list of expressions from
    /// `from`, reads each through Readyset and from MySQL, and fails with every difference.
    pub(crate) async fn compare(
        columns: &[(&str, &str)],
        rows: &[&str],
        from: &str,
        statements: Vec<Vec<String>>,
    ) {
        readyset_tracing::init_test_logging();
        let db = derive_test_name();
        mysql_helpers::recreate_database(&db).await;
        let upstream_opts: Opts = mysql_helpers::upstream_config().db_name(Some(&db)).into();
        let mut upstream = Conn::new(upstream_opts.clone()).await.unwrap();
        let columns = columns
            .iter()
            .map(|(name, ty)| format!("{name} {ty}"))
            .collect::<Vec<_>>()
            .join(", ");
        let rows = rows
            .iter()
            .enumerate()
            .map(|(id, row)| format!("({id}, {row})"))
            .collect::<Vec<_>>()
            .join(", ");
        upstream
            .query_drop(format!(
                "CREATE TABLE m (id int PRIMARY KEY, {columns}); INSERT INTO m VALUES {rows}"
            ))
            .await
            .unwrap();
        let (opts, _handle, shutdown_tx) = TestBuilder::default()
            .recreate_database(false)
            .migration_mode(MigrationMode::OutOfBand)
            .fallback(true)
            .parsing_preset(ParsingPreset::for_prod())
            .replicate_db(&db)
            .build::<MySQLAdapter>()
            .await;
        let mut readyset = Conn::new(opts.clone()).await.unwrap();

        let mut mismatches = Vec::new();
        let mut cached = Vec::new();
        for exprs in statements {
            let sql = select(from, &exprs);
            match readyset
                .query_drop(format!("CREATE CACHE FROM {sql}"))
                .await
            {
                Ok(()) => cached.push(exprs),
                Err(err) => mismatches.push(format!("{sql}: Readyset doesn't cache it: {err}")),
            }
        }
        sleep().await;

        let (readyset, upstream) = (Pool::new(opts), Pool::new(upstream_opts));
        let found: Vec<String> = stream::iter(cached)
            .map(|exprs| {
                let (readyset, upstream) = (readyset.clone(), upstream.clone());
                async move {
                    let mut readyset = readyset.get_conn().await.unwrap();
                    let mut upstream = upstream.get_conn().await.unwrap();
                    check(&mut readyset, &mut upstream, from, exprs).await
                }
            })
            .buffer_unordered(PARALLEL)
            .concat()
            .await;
        mismatches.extend(found);
        mismatches.sort();
        assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));

        shutdown_tx.shutdown().await;
    }
}

pub(crate) mod postgres {
    use futures::{StreamExt, stream};
    use readyset_adapter::backend::MigrationMode;
    use readyset_client_metrics::QueryDestination;
    use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
    use readyset_client_test_helpers::{Adapter, TestBuilder, derive_test_name, sleep};
    use readyset_data::DfValue;
    use readyset_sql_parsing::ParsingPreset;
    use tokio_postgres::Client;
    use tokio_postgres::types::Type;

    use super::PARALLEL;

    /// An expression's result: its type and its value in each row. A numeric shows all its digits
    /// after the point, as Postgres writes it.
    #[derive(Debug, PartialEq)]
    struct Column {
        ty: Type,
        values: Vec<Option<String>>,
    }

    /// Returns the statement selecting the id and `exprs` from `from`.
    fn select(from: &str, exprs: &[String]) -> String {
        format!("SELECT id, {} FROM {from}", exprs.join(", "))
    }

    /// Reads `sql` and returns each column after the id, with the rows in id order, or the error.
    async fn read(client: &Client, sql: &str) -> Result<Vec<Column>, String> {
        let error = |err: tokio_postgres::Error| match err.code() {
            Some(code) => format!("error {}", code.code()),
            None => err.to_string(),
        };
        let statement = client.prepare(sql).await.map_err(error)?;
        let mut rows = client.query(&statement, &[]).await.map_err(error)?;
        rows.sort_by_key(|row| row.get::<_, i32>(0));
        // A zero numeric shows its digits after the point only when asked for them.
        let show = |value: DfValue| match value {
            DfValue::Numeric(d) if d.is_zero() => {
                let scale = d.scale().unwrap_or(0).max(0) as usize;
                format!("{d:.scale$}")
            }
            value => value.to_string(),
        };
        Ok((1..statement.columns().len())
            .map(|i| Column {
                ty: statement.columns()[i].type_().clone(),
                values: rows
                    .iter()
                    .map(|row| match row.try_get::<_, Option<DfValue>>(i) {
                        Ok(value) => value.map(show),
                        Err(err) => Some(format!("{err:?}")),
                    })
                    .collect(),
            })
            .collect())
    }

    /// Reads the statement selecting `exprs` through Readyset and from Postgres, and returns each
    /// expression with a different type or values, and whether Readyset sent the read to Postgres.
    async fn check(
        readyset: &Client,
        upstream: &Client,
        from: &str,
        exprs: Vec<String>,
    ) -> Vec<String> {
        let sql = select(from, &exprs);
        let want = read(upstream, &sql).await;
        let got = read(readyset, &sql).await;
        let (got, want) = match (got, want) {
            (Ok(got), Ok(want)) => (got, want),
            (got, want) if got == want => return Vec::new(),
            (got, want) => return vec![format!("{sql}: Readyset {got:?}, Postgres {want:?}")],
        };
        let mut mismatches = Vec::new();
        let destination = last_query_info(readyset).await.destination;
        if !matches!(destination, QueryDestination::Readyset(_)) {
            mismatches.push(format!("{sql}: Readyset sent it to Postgres"));
        }
        for ((expr, got), want) in exprs.iter().zip(got).zip(want) {
            if got != want {
                mismatches.push(format!("{expr}: Readyset {got:?}, Postgres {want:?}"));
            }
        }
        mismatches
    }

    /// Fills the table `m` with `rows`, caches a statement selecting each list of expressions from
    /// `from`, reads each through Readyset and from Postgres, and fails with every difference.
    pub(crate) async fn compare(
        columns: &[(&str, &str)],
        rows: &[&str],
        from: &str,
        statements: Vec<Vec<String>>,
    ) {
        readyset_tracing::init_test_logging();
        let db = derive_test_name();
        PostgreSQLAdapter::recreate_database(&db).await;
        let mut config = psql_helpers::upstream_config();
        config.dbname(&db);
        let upstream = psql_helpers::connect(config.clone()).await;
        let columns = columns
            .iter()
            .map(|(name, ty)| format!("{name} {ty}"))
            .collect::<Vec<_>>()
            .join(", ");
        let rows = rows
            .iter()
            .enumerate()
            .map(|(id, row)| format!("({id}, {row})"))
            .collect::<Vec<_>>()
            .join(", ");
        upstream
            .batch_execute(&format!(
                "CREATE TABLE m (id int PRIMARY KEY, {columns}); INSERT INTO m VALUES {rows}"
            ))
            .await
            .unwrap();
        let (opts, _handle, shutdown_tx) = TestBuilder::default()
            .recreate_database(false)
            .migration_mode(MigrationMode::OutOfBand)
            .fallback(true)
            .parsing_preset(ParsingPreset::for_prod())
            .replicate_db(&db)
            .build::<PostgreSQLAdapter>()
            .await;
        let readyset = psql_helpers::connect(opts.clone()).await;

        let mut mismatches = Vec::new();
        let mut cached = Vec::new();
        for exprs in statements {
            let sql = select(from, &exprs);
            match readyset
                .simple_query(&format!("CREATE CACHE FROM {sql}"))
                .await
            {
                Ok(_) => cached.push(exprs),
                Err(err) => mismatches.push(format!("{sql}: Readyset doesn't cache it: {err}")),
            }
        }
        sleep().await;

        let found: Vec<String> = stream::iter(cached)
            .map(|exprs| {
                let (opts, config) = (opts.clone(), config.clone());
                async move {
                    let readyset = psql_helpers::connect(opts).await;
                    let upstream = psql_helpers::connect(config).await;
                    check(&readyset, &upstream, from, exprs).await
                }
            })
            .buffer_unordered(PARALLEL)
            .concat()
            .await;
        mismatches.extend(found);
        mismatches.sort();
        assert!(mismatches.is_empty(), "\n{}", mismatches.join("\n"));

        shutdown_tx.shutdown().await;
    }
}
