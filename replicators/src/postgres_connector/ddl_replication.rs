use std::str::FromStr;

use nom_sql::{DropTableStatement, Table};
use noria_errors::{internal, internal_err, ReadySetError, ReadySetResult};
use pgsql::tls::MakeTlsConnect;
use tokio_postgres as pgsql;
use tracing::debug;

use super::wal_reader::WalEvent;

const DDL_REPLICATION_LOG_TABLE: &str = "ddl_replication_log";

/// Setup everything in the database that's necessary for DDL replication.
///
/// This makes a new connection to the database, since the main connection created for the
/// replicator has the `replication` flag set, and postgres disallows schema change queries from
/// being run on replication connections
pub(crate) async fn setup_ddl_replication<T>(config: pgsql::Config, tls: T) -> ReadySetResult<()>
where
    T: MakeTlsConnect<pgsql::Socket> + Send,
    <T as MakeTlsConnect<pgsql::Socket>>::Stream: Send + 'static,
{
    let (client, conn) = config.connect(tls).await?;
    let conn_handle = tokio::spawn(conn);
    debug!("Setting up DDL replication");
    client
        .batch_execute(include_str!("./ddl_replication.sql"))
        .await?;
    debug!("Set up DDL replication");
    conn_handle.abort();

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DdlEventKind {
    CreateTable,
    DropTable,
    AlterTable,
    CreateView,
    DropView,
}

impl FromStr for DdlEventKind {
    type Err = ReadySetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "create_table" => Ok(Self::CreateTable),
            "drop_table" => Ok(Self::DropTable),
            "alter_table" => Ok(Self::AlterTable),
            "create_view" => Ok(Self::CreateView),
            "drop_view" => Ok(Self::DropView),
            s => internal!("Unknown DDL event kind `{}`", s),
        }
    }
}

pub(crate) struct DdlEvent<'a> {
    kind: DdlEventKind,
    schema_name: &'a str,
    object_name: &'a str,
}

impl<'a> DdlEvent<'a> {
    /// If the given WAL event represents a DDL change event, attempt to convert that to a
    /// [`DdlEvent`] and return it, otherwise returns `Ok(None)`. Returns an error if the WAL event
    /// represents an invalid [`DdlEvent`]
    pub(crate) fn from_wal_event(wal_event: &'a WalEvent) -> ReadySetResult<Option<Self>> {
        let tuple = if let WalEvent::Insert { table, tuple } = wal_event {
            if table != DDL_REPLICATION_LOG_TABLE {
                return Ok(None);
            }
            tuple
        } else {
            return Ok(None);
        };

        // Schema is:
        // CREATE TABLE IF NOT EXISTS readyset.ddl_replication_log (
        //     "id" SERIAL PRIMARY KEY,
        //     "event_type" TEXT NOT NULL,
        //     "schema_name" TEXT NOT NULL ,
        //     "object_name" TEXT NOT NULL,
        //     "created_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
        // );

        if tuple.len() != 5 {
            internal!("Invalid event received from DDL replication log");
        }

        let kind = DdlEventKind::from_str((&tuple[1]).try_into()?)
            .map_err(|_| internal_err("Invalid DDL event kind"))?;
        let schema_name = (&tuple[2]).try_into()?;
        let object_name = (&tuple[3]).try_into()?;

        Ok(Some(Self {
            kind,
            schema_name,
            object_name,
        }))
    }

    pub(crate) fn to_ddl(&self) -> String {
        match self.kind {
            DdlEventKind::DropTable => DropTableStatement {
                tables: vec![Table {
                    schema: Some(self.schema_name.into()),
                    name: self.object_name.into(),
                    alias: None,
                }],
                // We might be getting a drop table event for a table we don't have, eg if the table
                // originally failed to parse
                if_exists: true,
            }
            .to_string(),
            DdlEventKind::DropView => todo!(),
            DdlEventKind::CreateTable | DdlEventKind::AlterTable | DdlEventKind::CreateView => {
                todo!()
            }
        }
    }
}
