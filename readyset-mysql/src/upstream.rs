use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{self, ErrorKind};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use metrics::gauge;
use mysql_async::consts::{CapabilityFlags, Command, StatusFlags};
use mysql_async::prelude::Queryable;
use mysql_async::{
    ChangeUserOpts, Column, Conn, Opts, OptsBuilder, ResultSetStream, Row, UrlError,
};
use mysql_srv::{MsqlSrvError, QueryResultWriter};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::runtime::RuntimeFlavor;
use tracing::{debug, error, info_span, Instrument};

use database_utils::tls::{get_mysql_tls_config, ServerCertVerification};
use readyset_adapter::upstream_database::{
    fingerprint_rows, AclProbeOutcome, Refresh, UpstreamDestination, UpstreamStatementId,
};
use readyset_adapter::{UpstreamConfig, UpstreamDatabase, UpstreamPrepare};
use readyset_adapter_types::{DeallocateId, PreparedStatementType};
use readyset_client_metrics::QueryDestination;
use readyset_data::encoding::Encoding;
use readyset_data::upstream_system_props::{UpstreamCollation, DEFAULT_TIMEZONE_NAME};
use readyset_data::DfValue;
use readyset_errors::{internal, unsupported, ReadySetError, ReadySetResult};
use readyset_shallow::{CacheInsertGuard, ContentHash, MySqlMetadata, QueryMetadata};
use readyset_sql::ast::{Relation, SqlIdentifier};
use readyset_sql::Dialect;
use readyset_util::hash::hash;
use readyset_util::SizeOf;

use crate::backend::write_query_results;
use crate::{handle_error, Error};

type StatementID = u32;

/// MySQL server error codes that signal a privilege denial at prepare time:
/// ER_DBACCESS_DENIED_ERROR, ER_TABLEACCESS_DENIED_ERROR,
/// ER_COLUMNACCESS_DENIED_ERROR, ER_SPECIFIC_ACCESS_DENIED_ERROR,
/// ER_PROCACCESS_DENIED_ERROR.
fn is_privilege_error(code: u16) -> bool {
    matches!(code, 1044 | 1142 | 1143 | 1227 | 1370)
}

const ER_UNKNOWN_CHARACTER_SET: u16 = 1115;
const ER_UNKNOWN_COLLATION: u16 = 1273;
const ER_BAD_DB_ERROR: u16 = 1049;

/// Extract the server error code from a raw mysql_async error, if it is a server error.
fn server_error_code(error: &mysql_async::Error) -> Option<u16> {
    match error {
        mysql_async::Error::Server(e) => Some(e.code),
        _ => None,
    }
}

fn trim_leading_sql_comments(mut query: &str) -> &str {
    loop {
        query = query.trim_start();

        if let Some(comment) = query.strip_prefix("--") {
            query = comment
                .split_once('\n')
                .map(|(_, rest)| rest)
                .unwrap_or_default();
        } else if let Some(comment) = query.strip_prefix('#') {
            query = comment
                .split_once('\n')
                .map(|(_, rest)| rest)
                .unwrap_or_default();
        } else if let Some(comment) = query.strip_prefix("/*") {
            query = comment
                .split_once("*/")
                .map(|(_, rest)| rest)
                .unwrap_or_default();
        } else {
            return query;
        }
    }
}

fn take_sql_keyword(query: &str) -> Option<(&str, &str)> {
    let end = query
        .char_indices()
        .find_map(|(idx, ch)| (!ch.is_ascii_alphabetic()).then_some(idx))
        .unwrap_or(query.len());

    (end > 0).then(|| query.split_at(end))
}

fn is_create_database_statement(query: &str) -> bool {
    let query = trim_leading_sql_comments(query);
    let Some((create, query)) = take_sql_keyword(query) else {
        return false;
    };

    if !create.eq_ignore_ascii_case("CREATE") {
        return false;
    }

    let query = trim_leading_sql_comments(query);
    let Some((object_type, _)) = take_sql_keyword(query) else {
        return false;
    };

    object_type.eq_ignore_ascii_case("DATABASE") || object_type.eq_ignore_ascii_case("SCHEMA")
}

/// Extract the granted roles from `SHOW GRANTS` output. Role grants are the
/// rows without an ON clause, e.g. ``GRANT `r1`@`%`,`r2`@`%` TO `u`@`h` ``.
fn granted_roles(grants: &[String]) -> Vec<String> {
    grants
        .iter()
        .filter(|g| !g.contains(" ON "))
        .filter_map(|g| {
            let rest = g.strip_prefix("GRANT ")?;
            let (roles, _) = rest.split_once(" TO ")?;
            Some(roles.split(',').map(|r| r.trim().to_string()))
        })
        .flatten()
        .collect()
}

/// One row of a shallow cache entry. Entries are keyed per results charset and filled from
/// results the upstream converted into that charset. Text values are stored as canonical UTF-8
/// [`DfValue`]s and encoded back into the key's charset when served.
#[derive(Debug)]
pub enum CacheEntry {
    Text(Vec<DfValue>),
    Binary(Vec<DfValue>),
}

impl SizeOf for CacheEntry {
    fn deep_size_of(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Text(values) | Self::Binary(values) => values.deep_size_of(),
            }
    }

    fn size_is_empty(&self) -> bool {
        match self {
            Self::Text(values) | Self::Binary(values) => values.is_empty(),
        }
    }
}

impl ContentHash for CacheEntry {
    fn content_hash(&self) -> u64 {
        match self {
            Self::Text(values) => hash(&(0u8, values)),
            Self::Binary(values) => hash(&(1u8, values)),
        }
    }
}

/// Indicates the minimum upstream server version that we currently support. Used to error out
/// during connection phase if the version for the upstream server is too low.
const MIN_UPSTREAM_MAJOR_VERSION: u16 = 5;
const MIN_UPSTREAM_MINOR_VERSION: u16 = 7;

fn dt_to_value_params(dt: &[DfValue]) -> ReadySetResult<Vec<mysql_async::Value>> {
    dt.iter().map(|v| v.try_into()).collect()
}

/// Convert an upstream wire value to the [`DfValue`] stored in a shallow cache entry. Values of
/// text columns are decoded from the column's charset to UTF-8, so entries store canonical text
/// regardless of the charset the upstream converted the result into. Values of binary columns
/// (charset 63) are kept as raw bytes.
fn cache_df_value(col: &mysql_async::Value, column_charset: u16) -> io::Result<DfValue> {
    if let mysql_async::Value::Bytes(bytes) = col {
        let encoding = Encoding::from_mysql_collation_id(column_charset);
        if !matches!(encoding, Encoding::Binary) {
            return encoding.decode(bytes).map(DfValue::from).map_err(|e| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("failed decoding {col:?} as {encoding}: {e}"),
                )
            });
        }
    }
    col.try_into().map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("failed converting {col:?} to DfValue"),
        )
    })
}

#[pin_project(project = ReadResultStreamProj)]
#[derive(Debug)]
pub enum ReadResultStream<'a> {
    Text(#[pin] ResultSetStream<'a, 'a, 'static, Row, mysql_async::TextProtocol>),
    Binary(#[pin] ResultSetStream<'a, 'a, 'static, Row, mysql_async::BinaryProtocol>),
}

impl<'a> From<ResultSetStream<'a, 'a, 'static, Row, mysql_async::TextProtocol>>
    for ReadResultStream<'a>
{
    fn from(s: ResultSetStream<'a, 'a, 'static, Row, mysql_async::TextProtocol>) -> Self {
        ReadResultStream::Text(s)
    }
}

impl<'a> From<ResultSetStream<'a, 'a, 'static, Row, mysql_async::BinaryProtocol>>
    for ReadResultStream<'a>
{
    fn from(s: ResultSetStream<'a, 'a, 'static, Row, mysql_async::BinaryProtocol>) -> Self {
        ReadResultStream::Binary(s)
    }
}

#[derive(Debug)]
pub enum QueryResult<'a> {
    WriteResult {
        num_rows_affected: u64,
        // This field refers to the auto-increment ID that was generated for the most recent
        // INSERT operation on a table with an auto-incrementing primary key.
        // If no auto-increment column was involved, this value will be 0.
        last_inserted_id: u64,
        status_flags: StatusFlags,
    },
    ReadResult {
        stream: ReadResultStream<'a>,
        columns: Arc<[Column]>,
    },
    Command {
        status_flags: StatusFlags,
    },
}

impl UpstreamDestination for QueryResult<'_> {
    fn destination(&self) -> QueryDestination {
        QueryDestination::Upstream
    }
}

impl<'a> QueryResult<'a> {
    /// Process the query result, writing it to the given writer and optionally
    /// caching it.
    ///
    /// When `status_flags_override` is `Some(flags)`, those flags replace the
    /// flags that mysql-async extracted from the upstream response packets.
    /// This is the normal path for proxied results because mysql-async can
    /// produce garbage flags (e.g. due to PREPARE_Response mis-parsing).
    /// The override should be the "base" flags; mysql-srv will OR in
    /// `SERVER_MORE_RESULTS_EXISTS` when appropriate.
    ///
    /// When `status_flags_override` is `None`, the flags from mysql-async are
    /// forwarded verbatim (used only for cache-refresh paths that have no
    /// client writer).
    ///
    /// `results_encoding` is the results charset of the session (or, on a refresh, of the entry
    /// being refreshed), which the upstream connection's `character_set_results` mirrors. Shallow
    /// cache entries are keyed per results charset and filled from results the upstream converted
    /// into that charset. Cached copies of text values are decoded to UTF-8 and encoded back into
    /// the key's charset when served.
    pub async fn process<S>(
        self,
        writer: Option<QueryResultWriter<'_, S>>,
        mut cache: Option<CacheInsertGuard<readyset_adapter::shallow_key::ShallowKey, CacheEntry>>,
        status_flags_override: Option<StatusFlags>,
        results_encoding: Encoding,
    ) -> io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match self {
            QueryResult::Command { status_flags } => {
                let Some(writer) = writer else {
                    return Ok(());
                };
                let flags = status_flags_override.unwrap_or(status_flags);
                let rw = writer.start(&[]).await?;
                rw.set_status_flags(flags).finish().await
            }
            QueryResult::WriteResult {
                num_rows_affected,
                last_inserted_id,
                status_flags,
            } => {
                let Some(writer) = writer else {
                    return Ok(());
                };
                let flags = status_flags_override.unwrap_or(status_flags);
                write_query_results(
                    Ok((num_rows_affected, last_inserted_id)),
                    writer,
                    Some(flags),
                )
                .await
            }
            QueryResult::ReadResult {
                mut stream,
                columns,
            } => {
                let is_binary = matches!(stream, ReadResultStream::Binary(_));

                // Cache entries store text values as UTF-8 DfValues, decoded from the charset
                // each column's metadata reports. Skip filling the cache when that isn't
                // possible: a `binary` results charset suppresses the upstream's conversion to
                // a known charset, and an unsupported column charset can't be decoded.
                if matches!(results_encoding, Encoding::Binary | Encoding::OtherMySql(_))
                    || columns.iter().any(|c| {
                        matches!(
                            Encoding::from_mysql_collation_id(c.character_set()),
                            Encoding::OtherMySql(_)
                        )
                    })
                {
                    cache = None;
                }

                let formatted_cols = columns
                    .iter()
                    .map(|c| mysql_srv::Column::from_mysql(c, results_encoding))
                    .collect::<Vec<_>>();
                let mut rw = if let Some(writer) = writer {
                    Some(writer.start(&formatted_cols).await?)
                } else {
                    None
                };

                while let Some(row) = stream.next().await {
                    let row = match row {
                        Ok(row) => row,
                        Err(err) => {
                            if let Some(rw) = rw {
                                return handle_error!(Error::MySql(err), rw);
                            } else {
                                return Err(io::Error::other(format!("MySQL error: {err:?}")));
                            }
                        }
                    };

                    let mut copy = cache
                        .as_ref()
                        .map(|_| Vec::with_capacity(row.columns_ref().len()));
                    for i in 0..row.columns_ref().len() {
                        let col = row.as_ref(i).expect("Must match column number");

                        if let Some(ref mut copy) = copy {
                            copy.push(cache_df_value(col, row.columns_ref()[i].character_set())?);
                        }

                        if let Some(ref mut rw) = rw {
                            rw.write_col(col)?;
                        }
                    }

                    if let Some(ref mut rw) = rw {
                        rw.end_row().await?;
                    }

                    if let (Some(cache), Some(copy)) = (cache.as_mut(), copy) {
                        let entry = if is_binary {
                            CacheEntry::Binary(copy)
                        } else {
                            CacheEntry::Text(copy)
                        };
                        cache.push(entry);
                    }
                }

                if let Some(mut rw) = rw {
                    let flags = status_flags_override.or_else(|| stream.status_flags());
                    if let Some(flags) = flags {
                        rw = rw.set_status_flags(flags);
                    }
                    rw.finish().await?;
                }

                if let Some(ref mut cache) = cache {
                    cache.set_metadata(QueryMetadata::MySql(MySqlMetadata {
                        columns: Arc::clone(&columns),
                        columns_encoding: results_encoding,
                    }));
                    drop(cache.filled());
                }

                Ok(())
            }
        }
    }
}

#[async_trait]
impl Refresh for QueryResult<'_> {
    type Entry = CacheEntry;

    async fn refresh(
        self,
        cache: CacheInsertGuard<readyset_adapter::shallow_key::ShallowKey, Self::Entry>,
        encoding: Encoding,
    ) -> io::Result<()> {
        self.process(
            None::<QueryResultWriter<'_, tokio::net::TcpStream>>,
            Some(cache),
            None, // No status flags override for cache refresh (no client writer)
            encoding,
        )
        .await
    }
}

/// A connector to an underlying mysql store. This is really just a wrapper for the mysql crate.
pub struct MySqlUpstream {
    conn: Conn,
    prepared_statements: HashMap<StatementID, mysql_async::Statement>,
}

#[derive(Debug, Clone)]
pub struct StatementMeta {
    /// Metadata about the query parameters for this statement
    pub params: Vec<Column>,
    /// Metadata about the types of the columns in the rows returned by this statement
    pub schema: Vec<Column>,
}

impl Stream for ReadResultStream<'_> {
    type Item = Result<Row, mysql_async::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.project() {
            ReadResultStreamProj::Text(s) => s.poll_next(cx),
            ReadResultStreamProj::Binary(s) => s.poll_next(cx),
        }
    }
}

impl ReadResultStream<'_> {
    pub fn status_flags(&self) -> Option<StatusFlags> {
        match self {
            ReadResultStream::Text(s) => s.ok_packet().map(|o| o.status_flags()),
            ReadResultStream::Binary(s) => s.ok_packet().map(|o| o.status_flags()),
        }
    }
}

macro_rules! handle_query_result {
    ($result:expr) => {{
        let columns = ($result).columns().ok_or_else(|| {
            ReadySetError::Internal("The mysql_async result was already consumed".to_string())
        })?;

        if columns.len() > 0 {
            Ok(QueryResult::ReadResult {
                stream: $result
                    .stream_and_drop()
                    .await?
                    .ok_or_else(|| {
                        ReadySetError::Internal(
                            "The mysql_async resultset was already consumed".to_string(),
                        )
                    })?
                    .into(),
                columns,
            })
        } else {
            // Kinda sad that can't get status from conn, since it is mutably borrowed above
            let resultset = $result.stream_and_drop::<Row>().await?.ok_or_else(|| {
                ReadySetError::Internal("The mysql_async result has no resultsets".to_string())
            })?;

            Ok(QueryResult::WriteResult {
                num_rows_affected: resultset.affected_rows(),
                last_inserted_id: resultset.last_insert_id().unwrap_or(0),
                status_flags: resultset
                    .ok_packet()
                    .ok_or_else(|| {
                        ReadySetError::Internal(
                            "The mysql_async result has no ok packet".to_string(),
                        )
                    })?
                    .status_flags(),
            })
        }
    }};
}

impl MySqlUpstream {
    async fn query_without_default_database<'a>(
        query: &'a str,
        opts: &Opts,
    ) -> Result<QueryResult<'a>, Error> {
        let mut conn =
            Conn::new(OptsBuilder::from_opts(opts.clone()).db_name(None::<String>)).await?;

        let result = conn.query_iter(query).await?;
        let columns = result.columns().ok_or_else(|| {
            ReadySetError::Internal("The mysql_async result was already consumed".to_string())
        })?;

        if columns.len() > 0 {
            internal!("CREATE DATABASE/SCHEMA returned an unexpected result set");
        }

        let resultset = result.stream_and_drop::<Row>().await?.ok_or_else(|| {
            ReadySetError::Internal("The mysql_async result has no resultsets".to_string())
        })?;

        Ok(QueryResult::WriteResult {
            num_rows_affected: resultset.affected_rows(),
            last_inserted_id: resultset.last_insert_id().unwrap_or(0),
            status_flags: resultset
                .ok_packet()
                .ok_or_else(|| {
                    ReadySetError::Internal("The mysql_async result has no ok packet".to_string())
                })?
                .status_flags(),
        })
    }

    async fn connect_inner(
        upstream_config: UpstreamConfig,
        username: Option<String>,
        password: Option<String>,
        interactive: bool,
    ) -> Result<(Conn, HashMap<StatementID, mysql_async::Statement>), Error> {
        let url = upstream_config
            .upstream_db_url
            .as_deref()
            .ok_or(ReadySetError::InvalidUpstreamDatabase)?;

        let mut builder = {
            let opts = Opts::from_url(url)
                .map_err(|e: UrlError| Error::MySql(mysql_async::Error::Url(e)))?;
            OptsBuilder::from_opts(opts)
                .stmt_cache_size(0)
                .prefer_socket(false)
        };

        let ssl_opts = get_mysql_tls_config(&ServerCertVerification::from(&upstream_config).await?);
        if let Some(ssl_opts) = ssl_opts {
            builder = builder.ssl_opts(ssl_opts);
        }

        if let Some(username) = username {
            builder = builder.user(Some(username));
        }
        if let Some(password) = password {
            builder = builder.pass(Some(password));
        }
        if let Some(program_name) = upstream_config.program_name.as_deref() {
            builder = builder.connect_attributes(HashMap::from([(
                "_program_name".to_string(),
                program_name.to_string(),
            )]));
        }
        // Mirror the client's CLIENT_INTERACTIVE capability so the upstream session honors
        // interactive_timeout rather than wait_timeout when the client is interactive.
        if interactive {
            builder = builder.add_capability(CapabilityFlags::CLIENT_INTERACTIVE);
        }
        let opts: Opts = builder.into();
        let span = info_span!(
            "Connecting to MySQL upstream",
            host = %opts.ip_or_hostname(),
            port = %opts.tcp_port(),
            user = %opts.user().unwrap_or("<NO USER>"),
        );
        span.in_scope(|| debug!("Establishing connection"));
        let conn = Conn::new(OptsBuilder::from_opts(opts))
            .instrument(span.clone())
            .await?;

        // Check that the server version is supported.
        let (major, minor, _) = conn.server_version();
        if major < MIN_UPSTREAM_MAJOR_VERSION
            || (major == MIN_UPSTREAM_MAJOR_VERSION && minor < MIN_UPSTREAM_MINOR_VERSION)
        {
            return Err(Error::ReadySet(ReadySetError::UnsupportedServerVersion {
                major,
                minor: minor.to_string(),
                min_major: MIN_UPSTREAM_MAJOR_VERSION,
                min_minor: MIN_UPSTREAM_MINOR_VERSION,
            }));
        }

        span.in_scope(|| debug!("Established connection to upstream"));
        gauge!(metric::CLIENT_UPSTREAM_CONNECTIONS).increment(1.0);
        let prepared_statements = HashMap::new();
        Ok((conn, prepared_statements))
    }

    /// Look up the given session variable's collation in the upstream's
    /// information_schema.COLLATIONS.
    async fn collation_for(&mut self, variable: &str) -> Result<UpstreamCollation, Error> {
        let result: Option<(u16, String, String)> = self
            .conn
            .query_first(format!(
                "SELECT ID, CHARACTER_SET_NAME, COLLATION_NAME \
                 FROM information_schema.COLLATIONS \
                 WHERE COLLATION_NAME = @@{variable}"
            ))
            .await?;
        let Some((id, character_set_name, collation_name)) = result else {
            internal!("@@{variable} value is missing from information_schema.COLLATIONS")
        };
        if !valid_mysql_name(&character_set_name) || !valid_mysql_name(&collation_name) {
            internal!("upstream returned an invalid charset or collation name")
        }
        Ok(UpstreamCollation {
            id,
            character_set_name,
            collation_name,
        })
    }

    /// Run SET NAMES for the given charset and optional collation. Readyset decodes client
    /// bytes and sends UTF-8 statement text upstream, so the upstream's character_set_client
    /// must stay utf8mb4. SET options apply left to right, which lets a trailing assignment
    /// restore it within the same statement.
    async fn set_names(
        &mut self,
        charset: &str,
        collation: Option<&str>,
    ) -> Result<(), mysql_async::Error> {
        let names = match collation {
            Some(collation) => format!("'{charset}' COLLATE '{collation}'"),
            None => format!("'{charset}'"),
        };
        self.conn
            .query_drop(format!(
                "SET NAMES {names}, @@SESSION.character_set_client = 'utf8mb4'"
            ))
            .await
    }

    /// Set the session to the upstream's server default charset and collation and return
    /// what was applied.
    async fn set_server_default_charset(&mut self) -> Result<UpstreamCollation, Error> {
        let default = self.collation_for("collation_server").await?;
        self.set_names(&default.character_set_name, Some(&default.collation_name))
            .await?;
        Ok(default)
    }
}

#[async_trait]
impl UpstreamDatabase for MySqlUpstream {
    type QueryResult<'a> = QueryResult<'a>;
    type StatementMeta = StatementMeta;
    type PrepareData<'a> = ();
    type ExecMeta = ();
    type CacheEntry = CacheEntry;
    type ShallowExecMeta = ();
    type Error = Error;
    const DEFAULT_DB_VERSION: &'static str = "8.0.26-readyset\0";
    const SQL_DIALECT: readyset_sql::Dialect = readyset_sql::Dialect::MySQL;

    async fn connect(
        upstream_config: UpstreamConfig,
        username: Option<String>,
        password: Option<String>,
        interactive: bool,
    ) -> Result<Self, Error> {
        let (conn, prepared_statements) =
            Self::connect_inner(upstream_config, username, password, interactive).await?;
        Ok(Self {
            conn,
            prepared_statements,
        })
    }

    fn database(&self) -> Option<&str> {
        self.conn.opts().db_name()
    }

    fn version(&self) -> String {
        // The server's version relayed back to the client as the current server version. Most
        // clients will interpret the version numbers and use that to dictate which dialect they
        // send us. Anything after the version can be any text we desire. Additionally, the version
        // string must be null terminated.
        let (major, minor, patch) = self.conn.server_version();
        format!("{major}.{minor}.{patch}-readyset\0")
    }

    async fn reset(&mut self) -> Result<(), Self::Error> {
        self.conn.reset().await?;
        Ok(())
    }

    async fn is_connected(&mut self) -> Result<bool, Self::Error> {
        Ok(self.conn.ping().await.is_ok())
    }

    async fn ping(&mut self) -> Result<(), Self::Error> {
        self.conn.ping().await.map_err(Error::MySql)?;
        Ok(())
    }

    async fn change_user(
        &mut self,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<(), Self::Error> {
        self.conn
            .change_user(
                ChangeUserOpts::default()
                    .with_user(Some(user.to_string()))
                    .with_pass(Some(password.to_string()))
                    .with_db_name(Some(database.to_string())),
            )
            .await
            .map_err(Error::MySql)?;
        Ok(())
    }

    async fn can_prepare<S>(&mut self, query: S) -> anyhow::Result<()>
    where
        S: AsRef<str> + Send + Sync,
    {
        self.conn.prep(query.as_ref()).await?;
        Ok(())
    }

    fn is_privilege_error(error: &Self::Error) -> bool {
        matches!(error, Error::MySql(mysql_async::Error::Server(e)) if is_privilege_error(e.code))
    }

    async fn acl_probe(
        &mut self,
        query: &str,
        _n_params: usize,
        as_role: Option<&str>,
    ) -> Result<AclProbeOutcome, Self::Error> {
        if as_role.is_some() {
            internal!("MySQL ACL probes do not support assuming a role");
        }
        // MySQL checks privileges at prepare time, so the prepare is the probe.
        match self.conn.prep(query).await {
            Ok(statement) => {
                self.conn.close(statement).await?;
                Ok(AclProbeOutcome::Authorized)
            }
            Err(mysql_async::Error::Server(ref e)) if is_privilege_error(e.code) => {
                Ok(AclProbeOutcome::Denied)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn grant_fingerprint(
        &mut self,
        _relations: &[Relation],
        as_role: Option<&str>,
    ) -> Result<u64, Self::Error> {
        if as_role.is_some() {
            internal!("MySQL grant fingerprints do not support assuming a role");
        }
        // The engine reports the session's own effective grants; granted roles
        // are expanded with USING so a change to a role's own privileges flips
        // the hash.
        let mut rows: Vec<String> = self.conn.query("SHOW GRANTS").await?;
        let roles = granted_roles(&rows);
        if !roles.is_empty() {
            let using = roles.join(", ");
            let expanded: Vec<String> = self
                .conn
                .query(format!("SHOW GRANTS FOR CURRENT_USER() USING {using}"))
                .await?;
            rows.extend(expanded);
        }
        Ok(fingerprint_rows(rows))
    }

    async fn assumable_roles(&mut self) -> Result<Vec<String>, Self::Error> {
        // Sessions that issue SET ROLE route off-cache in Phase 1, so the
        // matrix never keys MySQL identities by an assumed role.
        Ok(Vec::new())
    }

    /// Prepares the given query using the mysql connection. Note, queries are prepared on a
    /// per connection basis. They are not universal.
    async fn prepare<'a, 'b, S>(
        &'a mut self,
        query: S,
        _: (),
        statement_type: PreparedStatementType,
    ) -> Result<UpstreamPrepare<Self>, Error>
    where
        S: AsRef<str> + Send + Sync + 'a,
    {
        if matches!(statement_type, PreparedStatementType::Unnamed) {
            unsupported!("MySQL does not support unnamed prepared statements");
        }

        let statement = self.conn.prep(query.as_ref()).await?;
        if let Some(old_stmt) = self
            .prepared_statements
            .insert(statement.id(), statement.clone())
        {
            self.conn.close(old_stmt).await?;
        }
        Ok(UpstreamPrepare {
            statement_id: statement.id(),
            meta: StatementMeta {
                params: statement.params().to_owned(),
                schema: statement.columns().to_vec(),
            },
        })
    }

    async fn execute<'a>(
        &'a mut self,
        id: &UpstreamStatementId,
        params: &[DfValue],
        _exec_meta: &Self::ExecMeta,
    ) -> Result<Self::QueryResult<'a>, Error> {
        let params = dt_to_value_params(params)?;

        let result = self
            .conn
            .exec_iter(
                self.prepared_statements.get(id).ok_or(Error::ReadySet(
                    ReadySetError::PreparedStatementMissing { statement_id: *id },
                ))?,
                params,
            )
            .await?;
        handle_query_result!(result)
    }

    async fn remove_statement(&mut self, statement_id: DeallocateId) -> Result<(), Self::Error> {
        match statement_id {
            DeallocateId::Numeric(id) => match self.prepared_statements.remove(&id) {
                Some(statement) => self.conn.close(statement).await?,
                None => {
                    // It's highly unlikely that a numeric statement id was _not_
                    // prepared via the mysql wire protocol (COM_STMT_PREPARE), but
                    // send it to the upstream for completeness and let mysql complain
                    // if the id is not found.
                    self.conn
                        .query_drop(format!("DEALLOCATE PREPARE {id}"))
                        .await?;
                }
            },
            DeallocateId::Named(name) => {
                self.conn
                    .query_drop(format!("DEALLOCATE PREPARE {name}"))
                    .await?
            }
            DeallocateId::All => {
                unsupported!("MySQL does not support a DEALLOCATE ALL behavior");
            }
        }

        Ok(())
    }

    async fn set_results_character_set(&mut self, charset: &str) -> Result<(), Self::Error> {
        self.conn
            .query_drop(format!("SET character_set_results = {charset}"))
            .await?;
        Ok(())
    }

    async fn set_connection_charset(
        &mut self,
        charset: &str,
        collation: &str,
    ) -> Result<Option<UpstreamCollation>, Self::Error> {
        if !valid_mysql_name(charset) || !valid_mysql_name(collation) {
            internal!("invalid MySQL charset or collation name")
        }
        match self.set_names(charset, Some(collation)).await {
            Ok(()) => return Ok(None),
            Err(e) if server_error_code(&e) == Some(ER_UNKNOWN_COLLATION) => {
                debug!(
                    charset,
                    collation, "upstream rejected collation, using charset default"
                );
                match self.set_names(charset, None).await {
                    Ok(()) => return Ok(Some(self.collation_for("collation_connection").await?)),
                    Err(e)
                        if matches!(
                            server_error_code(&e),
                            Some(ER_UNKNOWN_COLLATION | ER_UNKNOWN_CHARACTER_SET)
                        ) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            Err(e) if server_error_code(&e) == Some(ER_UNKNOWN_CHARACTER_SET) => {
                // Pre-8.0 upstreams know the utf8mb3 charset only by its legacy name, so retry
                // with that before giving up on the requested charset.
                if let Some((charset, collation)) = legacy_utf8mb3_names(charset, collation) {
                    debug!(
                        charset,
                        collation, "upstream rejected charset, retrying with legacy names"
                    );
                    match self.set_names(charset, Some(&collation)).await {
                        Ok(()) => {
                            return Ok(Some(self.collation_for("collation_connection").await?))
                        }
                        Err(e)
                            if matches!(
                                server_error_code(&e),
                                Some(ER_UNKNOWN_COLLATION | ER_UNKNOWN_CHARACTER_SET)
                            ) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
        debug!(charset, collation, "using server default charset");
        self.set_server_default_charset().await.map(Some)
    }

    async fn query<'a>(&'a mut self, query: &'a str) -> Result<Self::QueryResult<'a>, Error> {
        let has_default_database = self.database().is_some();
        let opts = self.conn.opts().clone();

        match self.conn.query_iter(query).await {
            Ok(result) => handle_query_result!(result),
            Err(e)
                if server_error_code(&e) == Some(ER_BAD_DB_ERROR)
                    && has_default_database
                    && is_create_database_statement(query) =>
            {
                debug!("retrying CREATE DATABASE/SCHEMA without a default database");
                Self::query_without_default_database(query, &opts).await
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn query_ext<'a>(
        &'a mut self,
        query: &'a str,
        _exec_meta: &Self::ExecMeta,
    ) -> Result<Self::QueryResult<'a>, Error> {
        self.query(query).await
    }

    // MySQL does not have a separation of Simple/Extended query protocols like Postgres does.
    async fn simple_query<'a>(
        &'a mut self,
        _query: &'a str,
    ) -> Result<Self::QueryResult<'a>, Error> {
        unsupported!("MySQL does not have a simple_query protocol");
    }

    async fn start_tx<'a>(&'a mut self, query: &'a str) -> Result<Self::QueryResult<'a>, Error> {
        self.conn.query_drop(query).await?;

        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    async fn commit<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        let result = self.conn.query_iter("COMMIT").await?;
        result.drop_result().await?;

        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    async fn rollback<'a>(&'a mut self) -> Result<Self::QueryResult<'a>, Error> {
        let result = self.conn.query_iter("ROLLBACK").await?;
        result.drop_result().await?;

        Ok(QueryResult::Command {
            status_flags: self.conn.status(),
        })
    }

    async fn schema_search_path(&mut self) -> Result<Vec<SqlIdentifier>, Self::Error> {
        Ok(self.database().into_iter().map(|s| s.into()).collect())
    }

    async fn set_schema_search_path(&mut self, path: &[SqlIdentifier]) -> Result<(), Self::Error> {
        let database = match path {
            [] => internal!("Cannot set empty schema search path in MySQL"),
            [_, _, ..] => internal!("MySQL only supports using a single database at a time"),
            [db] => db,
        };

        let query = format!("USE {}", Dialect::MySQL.quote_identifier(database));
        debug!(%query, "Setting database on upstream");
        self.conn.query_drop(&query).await?;
        Ok(())
    }

    async fn timezone_name(&mut self) -> Result<SqlIdentifier, Self::Error> {
        Ok(DEFAULT_TIMEZONE_NAME.into())
    }

    async fn lower_case_table_names(&mut self) -> Result<bool, Self::Error> {
        let res: Vec<u8> = self.conn.query("select @@lower_case_table_names").await?;
        let [v] = &res[..] else {
            internal!("upstream is missing lower_case_table_names system variable");
        };
        match v {
            0 => Ok(false),
            1 | 2 => Ok(true),
            v => {
                error!("lower_case_table_names value {} is unsupported", v);
                Ok(false)
            }
        }
    }

    async fn lower_case_database_names(&mut self) -> Result<bool, Self::Error> {
        self.lower_case_table_names().await
    }

    async fn group_concat_max_len(&mut self) -> Result<usize, Self::Error> {
        let res: Vec<u64> = self.conn.query("select @@group_concat_max_len").await?;
        let [v] = &res[..] else {
            internal!("upstream is missing group_concat_max_len system variable");
        };
        Ok(*v as usize)
    }

    async fn server_default_collation(&mut self) -> Result<Option<UpstreamCollation>, Self::Error> {
        Ok(Some(self.collation_for("collation_server").await?))
    }

    async fn shallow_exec_meta(
        &mut self,
        _meta: &Self::ExecMeta,
    ) -> Result<Self::ShallowExecMeta, Self::Error> {
        Ok(())
    }

    fn is_meta_compatible(cache: &Self::CacheEntry) -> bool {
        matches!(cache, CacheEntry::Binary(_))
    }
}

fn valid_mysql_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// Map the modern utf8mb3 charset and collation names to the legacy "utf8" names, which are
/// the only names pre-8.0 upstreams recognize. Return `None` for other charsets.
fn legacy_utf8mb3_names(charset: &str, collation: &str) -> Option<(&'static str, String)> {
    if charset != "utf8mb3" {
        return None;
    }
    let suffix = collation.strip_prefix("utf8mb3")?;
    Some(("utf8", format!("utf8{suffix}")))
}

impl Drop for MySqlUpstream {
    fn drop(&mut self) {
        gauge!(metric::CLIENT_UPSTREAM_CONNECTIONS).decrement(1.0);
        // Properly close the connection unless this is a test using a single-threaded runtime
        let rt = tokio::runtime::Handle::current();
        if rt.runtime_flavor() != RuntimeFlavor::CurrentThread {
            tokio::task::block_in_place(|| {
                let _ = rt.block_on(self.conn.write_command_data(Command::COM_QUIT, &[]));
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_entry_content_hash() {
        let a = CacheEntry::Text(vec![DfValue::from(1), DfValue::from("x")]);
        let b = CacheEntry::Text(vec![DfValue::from(1), DfValue::from("x")]);
        let c = CacheEntry::Text(vec![DfValue::from(2), DfValue::from("x")]);
        assert_eq!(a.content_hash(), b.content_hash());
        assert_ne!(a.content_hash(), c.content_hash());

        let text = CacheEntry::Text(vec![DfValue::from(1)]);
        let binary = CacheEntry::Binary(vec![DfValue::from(1)]);
        assert_ne!(text.content_hash(), binary.content_hash());
    }

    #[test]
    fn granted_roles_skips_privilege_grants() {
        let grants = vec![
            "GRANT USAGE ON *.* TO `alice`@`%`".to_string(),
            "GRANT SELECT ON `db`.`t` TO `alice`@`%`".to_string(),
            "GRANT `r1`@`%`,`r2`@`%` TO `alice`@`%`".to_string(),
        ];
        assert_eq!(granted_roles(&grants), vec!["`r1`@`%`", "`r2`@`%`"]);
    }

    #[test]
    fn granted_roles_empty_without_role_grants() {
        let grants = vec!["GRANT SELECT ON `db`.`t` TO `alice`@`%`".to_string()];
        assert!(granted_roles(&grants).is_empty());
    }

    #[test]
    fn privilege_error_codes() {
        for code in [1044, 1142, 1143, 1227, 1370] {
            assert!(is_privilege_error(code));
        }
        // Connect-phase and transient errors are not privilege denials.
        for code in [1045, 1049, 1064, 2006] {
            assert!(!is_privilege_error(code));
        }
    }

    #[test]
    fn mysql_names_must_be_plain_identifiers() {
        assert!(valid_mysql_name("utf8mb4_0900_ai_ci"));
        assert!(!valid_mysql_name(""));
        assert!(!valid_mysql_name("utf8mb4'"));
        assert!(!valid_mysql_name("utf8mb4; SELECT 1"));
    }

    #[test]
    fn utf8mb3_names_map_to_legacy_utf8() {
        assert_eq!(
            legacy_utf8mb3_names("utf8mb3", "utf8mb3_general_ci"),
            Some(("utf8", "utf8_general_ci".to_string()))
        );
        assert_eq!(legacy_utf8mb3_names("utf8mb4", "utf8mb4_general_ci"), None);
        assert_eq!(legacy_utf8mb3_names("utf8mb3", "latin1_swedish_ci"), None);
    }
}
