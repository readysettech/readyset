//! Trait for interacting with an consensus system to determine
//! which ReadySet worker acts as the controller, which ReadySet workers exist, detecting failed
//! workers which necessitate changes, and storing cluster wide global state.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use clap::ValueEnum;
use enum_dispatch::enum_dispatch;
use readyset_data::{DfType, Dialect};
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::ast::{NonReplicatedRelation, Relation, SqlIdentifier};
use replication_offset::ReplicationOffset;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::error;
use url::Url;

mod local;
pub mod mcp_tokens;
mod standalone;

pub use self::local::{LocalAuthority, LocalAuthorityStore};
pub use self::mcp_tokens::{McpToken, McpTokenScope, McpTokenStore};
pub use self::standalone::StandaloneAuthority;
use crate::debug::stats::PersistentStats;
use crate::ControllerDescriptor;

// This should be an associated type on Authority but since Authority will only have one possible
// type inside of ReadySet, we are using a type alias here instead.
// If Authority ever moves out of ReadySet, it should become an associated type.
// LeaderPayload must be Serialize + DeserializeOwned + PartialEq
type LeaderPayload = ControllerDescriptor;

pub type WorkerId = String;

const CACHE_DDL_REQUESTS_PATH: &str = "cache_ddl_requests";
const SHALLOW_CACHE_DDL_REQUESTS_PATH: &str = "shallow_cache_ddl_requests";
const PERSISTENT_STATS_PATH: &str = "persistent_stats";
const SCHEMA_REPLICATION_OFFSET_PATH: &str = "schema_replication_offset";
const SCHEMA_CATALOG_PATH: &str = "schema_catalog";
const CUSTOM_TYPES_PATH: &str = "custom_types";
const NON_REPLICATED_RELATIONS_PATH: &str = "non_replicated_relations";

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct CacheDDLRequest {
    pub unparsed_stmt: String,
    pub schema_search_path: Vec<SqlIdentifier>,
    pub dialect: Dialect,
    #[serde(default)]
    pub cache_name: Option<Relation>,
}

/// One entry in the persisted schema catalog: canonical DDL text for a table or view that
/// Readyset has accepted from upstream.
///
/// At controller startup, each entry is re-parsed and replayed through `apply_changelist`
/// to rebuild the table and view portions of the in-memory schema registry.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SchemaCatalogEntry {
    /// Canonical SQL text (e.g. `CREATE TABLE "schema"."name" (...)`). Schema-qualified.
    pub unparsed_stmt: String,
    /// Schema search path under which `unparsed_stmt` should be re-parsed and resolved on replay.
    pub schema_search_path: Vec<SqlIdentifier>,
    /// Dialect under which `unparsed_stmt` should be re-parsed on replay.
    pub dialect: Dialect,
}

/// A user-defined custom type persisted for replay at controller startup.  (`CREATE TYPE` is
/// not handled by the parser.)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedCustomType {
    pub name: Relation,
    pub ty: DfType,
}

/// A response to a `worker_heartbeat`, to inform the worker of its
/// status within the system.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthorityWorkerHeartbeatResponse {
    Alive,
    Failed,
}

/// The set of possible results to retrieving the new leader.
#[derive(Debug, PartialEq, Eq)]
pub enum GetLeaderResult {
    NewLeader(LeaderPayload),
    Unchanged,
    NoLeader,
}

/// Restriction for how domains containing a particular kind of node can be scheduled onto a
/// particular worker.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub enum NodeTypeSchedulingRestriction {
    /// No restrictions: all domains with or without this node type may be scheduled onto this
    /// worker
    #[default]
    None,
    /// Only domains containing this node type may be scheduled onto this worker
    OnlyWithNodeType,
    /// Domains containing this node type may never be scheduled onto this worker
    NeverWithNodeType,
}

/// Configuration for how domains should be scheduled onto a particular worker.
///
/// The [`Default`] value for this struct allows any domain to be scheduled onto any worker.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct WorkerSchedulingConfig {
    /// Configuration for how domains containing or not containing reader nodes may be scheduled
    /// onto this worker
    pub reader_nodes: NodeTypeSchedulingRestriction,
}

/// Initial registration request body, sent from workers to controllers.
/// ///
/// (used for the `/worker_rx/register` route)
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkerDescriptor {
    /// URI at which the worker can be reached.
    pub worker_uri: Url,
    /// Socket address the worker listens on for data-plane read queries.
    pub reader_addr: SocketAddr,
    /// True if this worker may become a leader
    pub leader_eligible: bool,
    /// Configuration for how domains should be scheduled onto this worker
    pub domain_scheduling_config: WorkerSchedulingConfig,
}

pub trait UpdateInPlace<E, F, P>: Send + Sync
where
    F: FnMut(Option<&mut P>) -> Result<(), E>,
{
    /// An optimized method to update the controller state in place, the update is performed with
    /// the provided `f` closure.
    fn update_controller_in_place(&self, f: F) -> Result<(), E>;
}

#[async_trait]
#[enum_dispatch]
pub trait AuthorityControl: Send + Sync {
    /// If this [`AuthorityControl`] also implements the optimized [`UpdateInPlace`] trait, returns
    /// self as [`UpdateInPlace`], so caller can use optimized method instead.
    fn as_local<E, F, P>(&self) -> Option<&dyn UpdateInPlace<E, F, P>>
    where
        P: 'static,
        F: FnMut(Option<&mut P>) -> Result<(), E>,
    {
        None
    }

    /// Initializes the authority. This performs any initialization that the authority client
    /// needs to perform with the backend. This should be performed before any other
    /// calls are made to AuthorityControl functions.
    /// TODO(justin): Existing authorities should guarantee authority usage adheres to calling
    /// init() before other functionality.
    async fn init(&self) -> ReadySetResult<()>;

    /// Attempt to become leader with a specific payload. The payload should be something that can
    /// be deserialized to get the information on how to connect to the leader. If it is successful
    /// the this will return Some(payload), otherwise None and another instance has become leader.
    async fn become_leader(&self, payload: LeaderPayload) -> ReadySetResult<Option<LeaderPayload>>;

    /// Voluntarily give up leadership, allowing another node to become leader. It is extremely
    /// important that the node calling this function is actually the leader.
    async fn surrender_leadership(&self) -> ReadySetResult<()>;

    /// Returns the payload for the current leader, blocking if there is not currently a leader.
    /// This method is intended for clients to determine the current leader.
    async fn get_leader(&self) -> ReadySetResult<LeaderPayload>;

    /// Non-blocking call to retrieve a change in the authorities leader state.
    async fn try_get_leader(&self) -> ReadySetResult<GetLeaderResult>;

    /// Do a non-blocking read at the indicated key.
    async fn try_read<P>(&self, path: &str) -> ReadySetResult<Option<P>>
    where
        P: DeserializeOwned;

    // Temporarily here to support arbitrary introspection into the authority. Will replace with
    // better functions later.
    async fn try_read_raw(&self, path: &str) -> ReadySetResult<Option<Vec<u8>>>;

    /// Repeatedly attempts to do a read modify write operation. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    async fn read_modify_write<F, P, E>(&self, path: &str, f: F) -> ReadySetResult<Result<P, E>>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        P: Send + Serialize + DeserializeOwned,
        E: Send;

    /// Register a worker with their descriptor. Returns a unique identifier that represents this
    /// worker if successful.
    async fn register_worker(&self, payload: WorkerDescriptor) -> ReadySetResult<Option<WorkerId>>
    where
        WorkerDescriptor: Serialize;

    /// Workers run this function on a regular cadence to confirm current state. Returns a response
    /// for next actions for this particular worker or if it should continue being a worker.
    async fn worker_heartbeat(
        &self,
        id: WorkerId,
    ) -> ReadySetResult<AuthorityWorkerHeartbeatResponse>;

    /// Retrieves the current set of workers from the authority.
    async fn get_workers(&self) -> ReadySetResult<HashSet<WorkerId>>;

    /// Retrieves the worker data for a set of workers.
    async fn worker_data(
        &self,
        worker_ids: Vec<WorkerId>,
    ) -> ReadySetResult<HashMap<WorkerId, WorkerDescriptor>>;

    /// Repeatedly attempts to update the controller state. Each attempt consists of a read of
    /// the indicated node, a call to `f` with the data read (or None if the node did not exist),
    /// and finally a write back to the node if it hasn't changed from when it was originally
    /// written. The process aborts when a write succeeds or a call to `f` returns `Err`. In either
    /// case, returns the last value produced by `f`.
    /// In addition some implementors of the trait may apply the method `u` to the stored copy of
    /// the value only.
    /// `s` provides a mapping of the controller state to its schema replication offset--opaque at
    /// this level due to there being a cyclic dependency.
    async fn update_controller_state<F, S, U, P: 'static, R, E>(
        &self,
        f: F,
        s: S,
        u: U,
    ) -> ReadySetResult<Result<P, E>>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>, // How to change the ControllerState
        S: Send + Fn(&P) -> Option<R>,              // Extract ReplicationOffset
        U: Send + FnMut(&mut P),                    // Apply to the copy of ControllerState only
        P: Send + Serialize + DeserializeOwned + Clone, // opaque ControllerState
        R: Send + Serialize + DeserializeOwned + Clone, // opaque ReplicationOffset
        E: Send;

    /// Overwrite the controller state with the given value, without regard for what was stored
    /// there before.
    async fn overwrite_controller_state<P>(&self, state: P) -> ReadySetResult<()>
    where
        P: Send + Serialize + 'static;

    /// Return the list of cache ddl requests (CREATE CACHE or DROP CACHE) statements that have been
    /// run against this ReadySet deployment.
    ///
    /// This is stored separately from the controller state so that it's always available, using
    /// backwards-compatible serialization, for if the controller state can't be deserialized
    async fn cache_ddl_requests(&self) -> ReadySetResult<Vec<CacheDDLRequest>> {
        self.ddl_requests(CACHE_DDL_REQUESTS_PATH).await
    }

    async fn shallow_cache_ddl_requests(&self) -> ReadySetResult<Vec<CacheDDLRequest>> {
        self.ddl_requests(SHALLOW_CACHE_DDL_REQUESTS_PATH).await
    }

    async fn ddl_requests(&self, path: &str) -> ReadySetResult<Vec<CacheDDLRequest>> {
        Ok(self.try_read::<Vec<String>>(path)
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| match serde_json::from_slice(v.as_bytes()) {
                Ok(val) => Some(val),
                Err(err) => {
                    error!(%err, "Failed to deserialize CacheDDLRequest. Cache will not be re-created");
                    None
                }
            }).collect())
    }

    /// Insert a new cache ddl request that has been run
    /// against this ReadySet deployment
    ///
    /// These are stored separately from the controller state so that it's always available, using
    /// backwards-compatible serialization, for if the controller state can't be deserialized
    async fn add_cache_ddl_request(&self, cache_ddl_req: CacheDDLRequest) -> ReadySetResult<()> {
        let name = cache_ddl_req.cache_name.clone();
        let cache_ddl_req = serde_json::ser::to_string(&cache_ddl_req)?;
        modify_cache_ddl_requests(self, move |stmts| {
            if let Some(name) = &name {
                stmts.retain(|stmt| cache_ddl_request_name(stmt).as_ref() != Some(name));
            }
            stmts.push(cache_ddl_req.clone());
        })
        .await
    }

    async fn add_shallow_cache_ddl_request(
        &self,
        cache_ddl_req: CacheDDLRequest,
    ) -> ReadySetResult<()> {
        let name = cache_ddl_req.cache_name.clone();
        let cache_ddl_req = serde_json::ser::to_string(&cache_ddl_req)?;
        modify_shallow_cache_ddl_requests(self, move |stmts| {
            if let Some(name) = &name {
                stmts.retain(|stmt| cache_ddl_request_name(stmt).as_ref() != Some(name));
            }
            stmts.push(cache_ddl_req.clone());
        })
        .await
    }

    /// Removes the provided statement from the store.
    async fn remove_cache_ddl_request(&self, cache_ddl_req: CacheDDLRequest) -> ReadySetResult<()> {
        let cache_ddl_req = serde_json::ser::to_string(&cache_ddl_req)?;
        modify_cache_ddl_requests(self, move |stmts| {
            stmts.retain(|stmt| *stmt != cache_ddl_req);
        })
        .await
    }

    async fn remove_shallow_cache_ddl_request(
        &self,
        cache_ddl_req: CacheDDLRequest,
    ) -> ReadySetResult<()> {
        let cache_ddl_req = serde_json::ser::to_string(&cache_ddl_req)?;
        modify_shallow_cache_ddl_requests(self, move |stmts| {
            stmts.retain(|stmt| *stmt != cache_ddl_req);
        })
        .await
    }

    /// Removes `CREATE CACHE` request for `name`, returning whether any entry matched.
    async fn remove_cache_ddl_requests_named(&self, name: &Relation) -> ReadySetResult<bool> {
        let removed = Arc::new(AtomicBool::new(false));
        let flag = removed.clone();
        let name = name.clone();
        modify_cache_ddl_requests(self, move |stmts| {
            let before = stmts.len();
            stmts.retain(|stmt| cache_ddl_request_name(stmt).as_ref() != Some(&name));
            flag.store(stmts.len() != before, Ordering::Relaxed);
        })
        .await?;
        Ok(removed.load(Ordering::Relaxed))
    }

    /// Shallow counterpart to [`AuthorityControl::remove_cache_ddl_requests_named`].
    async fn remove_shallow_cache_ddl_requests_named(
        &self,
        name: &Relation,
    ) -> ReadySetResult<bool> {
        let removed = Arc::new(AtomicBool::new(false));
        let flag = removed.clone();
        let name = name.clone();
        modify_shallow_cache_ddl_requests(self, move |stmts| {
            let before = stmts.len();
            stmts.retain(|stmt| cache_ddl_request_name(stmt).as_ref() != Some(&name));
            flag.store(stmts.len() != before, Ordering::Relaxed);
        })
        .await?;
        Ok(removed.load(Ordering::Relaxed))
    }

    /// Removes all stored cache ddl requests
    async fn remove_all_cache_ddl_requests(&self) -> ReadySetResult<()> {
        modify_cache_ddl_requests(self, move |stmts| {
            stmts.clear();
        })
        .await
    }

    async fn remove_all_shallow_cache_ddl_requests(&self) -> ReadySetResult<()> {
        modify_shallow_cache_ddl_requests(self, move |stmts| {
            stmts.clear();
        })
        .await
    }

    /// Returns stats persisted in the authority. Wrapper around `Self::try_read`.
    async fn persistent_stats(&self) -> ReadySetResult<Option<PersistentStats>> {
        self.try_read(PERSISTENT_STATS_PATH).await
    }

    /// Update the stats persisted in the authority. Wrapper around `read_modify_write`.
    async fn update_persistent_stats<F>(&self, f: F) -> ReadySetResult<PersistentStats>
    where
        F: Send + FnMut(Option<PersistentStats>) -> ReadySetResult<PersistentStats>,
    {
        self.read_modify_write(PERSISTENT_STATS_PATH, f)
            .await
            .and_then(std::convert::identity)
    }

    /// Returns the stored schema [`ReplicationOffset`], if present. Wrapper around Self::try_read
    async fn schema_replication_offset(&self) -> ReadySetResult<Option<ReplicationOffset>> {
        self.try_read(SCHEMA_REPLICATION_OFFSET_PATH).await
    }

    /// Persists the schema [`ReplicationOffset`] to its own Authority key so it survives a
    /// controller restart and graph regeneration independently of any other state.
    async fn overwrite_schema_replication_offset(
        &self,
        offset: ReplicationOffset,
    ) -> ReadySetResult<()> {
        self.read_modify_write::<_, ReplicationOffset, ReadySetError>(
            SCHEMA_REPLICATION_OFFSET_PATH,
            move |_| Ok(offset.clone()),
        )
        .await??;
        Ok(())
    }

    /// Returns the persisted schema catalog: one entry per relation or type Readyset has
    /// accepted from upstream. At controller startup, these are re-parsed and replayed to
    /// rebuild the in-memory schema registry.
    ///
    /// Stored separately from the controller state so that an unreadable controller blob does
    /// not lose the schema; an individual unparseable entry is logged and skipped without
    /// blocking the rest of the catalog.
    async fn schema_catalog_entries(&self) -> ReadySetResult<Vec<SchemaCatalogEntry>> {
        Ok(self
            .try_read::<Vec<String>>(SCHEMA_CATALOG_PATH)
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| match serde_json::from_slice(v.as_bytes()) {
                Ok(val) => Some(val),
                Err(err) => {
                    error!(%err, "Failed to deserialize SchemaCatalogEntry; entry will not be replayed");
                    None
                }
            })
            .collect())
    }

    /// Replaces the persisted schema catalog with the given list.
    ///
    /// The catalog is rebuilt from the in-memory registry on every schema-mutating commit, so
    /// this is a wholesale overwrite rather than an append/remove pair.
    async fn overwrite_schema_catalog(
        &self,
        entries: Vec<SchemaCatalogEntry>,
    ) -> ReadySetResult<()> {
        let encoded: Vec<String> = entries
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()?;
        self.read_modify_write::<_, Vec<String>, ReadySetError>(SCHEMA_CATALOG_PATH, move |_| {
            Ok(encoded.clone())
        })
        .await??;
        Ok(())
    }

    /// Returns the persisted custom types Readyset has accepted from upstream.
    async fn custom_types(&self) -> ReadySetResult<Vec<PersistedCustomType>> {
        Ok(self
            .try_read::<Vec<String>>(CUSTOM_TYPES_PATH)
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| match serde_json::from_slice(v.as_bytes()) {
                Ok(val) => Some(val),
                Err(err) => {
                    error!(%err, "Failed to deserialize PersistedCustomType; entry will not be replayed");
                    None
                }
            })
            .collect())
    }

    /// Replaces the persisted custom types list.
    async fn overwrite_custom_types(
        &self,
        entries: Vec<PersistedCustomType>,
    ) -> ReadySetResult<()> {
        let encoded: Vec<String> = entries
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()?;
        self.read_modify_write::<_, Vec<String>, ReadySetError>(CUSTOM_TYPES_PATH, move |_| {
            Ok(encoded.clone())
        })
        .await??;
        Ok(())
    }

    /// Returns the persisted non-replicated relation markers: upstream relations Readyset knows
    /// about but does not replicate (excluded by configuration, partitioned, unsupported type,
    /// etc.). Persisted separately because these markers have no SQL DDL form.
    async fn non_replicated_relations(&self) -> ReadySetResult<Vec<NonReplicatedRelation>> {
        Ok(self
            .try_read::<Vec<String>>(NON_REPLICATED_RELATIONS_PATH)
            .await?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| match serde_json::from_slice(v.as_bytes()) {
                Ok(val) => Some(val),
                Err(err) => {
                    error!(%err, "Failed to deserialize NonReplicatedRelation; entry will not be replayed");
                    None
                }
            })
            .collect())
    }

    /// Replaces the persisted non-replicated relations list.
    async fn overwrite_non_replicated_relations(
        &self,
        entries: Vec<NonReplicatedRelation>,
    ) -> ReadySetResult<()> {
        let encoded: Vec<String> = entries
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()?;
        self.read_modify_write::<_, Vec<String>, ReadySetError>(
            NON_REPLICATED_RELATIONS_PATH,
            move |_| Ok(encoded.clone()),
        )
        .await??;
        Ok(())
    }
}

fn cache_ddl_request_name(raw: &str) -> Option<Relation> {
    serde_json::from_str::<CacheDDLRequest>(raw)
        .ok()
        .and_then(|req| req.cache_name)
}

async fn modify_ddl_requests<A, F>(authority: &A, path: &str, mut f: F) -> ReadySetResult<()>
where
    A: AuthorityControl + ?Sized,
    F: FnMut(&mut Vec<String>) + Send,
{
    authority
        .read_modify_write::<_, Vec<String>, ReadySetError>(path, move |stmts| {
            let mut stmts = stmts.unwrap_or_default();
            f(&mut stmts);
            Ok(stmts)
        })
        .await??;

    Ok(())
}

async fn modify_cache_ddl_requests<A, F>(authority: &A, f: F) -> ReadySetResult<()>
where
    A: AuthorityControl + ?Sized,
    F: FnMut(&mut Vec<String>) + Send,
{
    modify_ddl_requests(authority, CACHE_DDL_REQUESTS_PATH, f).await
}

async fn modify_shallow_cache_ddl_requests<A, F>(authority: &A, f: F) -> ReadySetResult<()>
where
    A: AuthorityControl + ?Sized,
    F: FnMut(&mut Vec<String>) + Send,
{
    modify_ddl_requests(authority, SHALLOW_CACHE_DDL_REQUESTS_PATH, f).await
}

/// Enum that dispatches calls to the `AuthorityControl` trait to
/// the respective variant.
#[allow(clippy::large_enum_variant)]
#[enum_dispatch(AuthorityControl)]
pub enum Authority {
    LocalAuthority,
    StandaloneAuthority,
}

impl Debug for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Authority::LocalAuthority(_) => f.write_str("LocalAuthority"),
            Authority::StandaloneAuthority(_) => f.write_str("StandaloneAuthority"),
        }
    }
}

impl Authority {
    pub fn is_single_process(&self) -> bool {
        matches!(
            self,
            Self::LocalAuthority(..) | Self::StandaloneAuthority(..)
        )
    }
}

/// Enum that mirrors Authority that parses command line arguments.
#[derive(Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum AuthorityType {
    Local,
    Standalone,
}

impl FromStr for AuthorityType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(AuthorityType::Local),
            "standalone" => Ok(AuthorityType::Standalone),
            other => Err(anyhow!("Invalid authority type: {}", other)),
        }
    }
}

impl Display for AuthorityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            AuthorityType::Local => write!(f, "local"),
            AuthorityType::Standalone => write!(f, "standalone"),
        }
    }
}

impl AuthorityType {
    pub fn to_authority(&self, addr: &str, deployment: &str) -> ReadySetResult<Authority> {
        match self {
            AuthorityType::Local => Ok(Authority::from(LocalAuthority::new())),
            AuthorityType::Standalone => Ok(Authority::from(
                StandaloneAuthority::new(addr, deployment).map_err(|e| {
                    ReadySetError::Internal(format!(
                        "failed to create standalone authority at '{}' for deployment '{}': {}",
                        addr, deployment, e
                    ))
                })?,
            )),
        }
    }
}

#[cfg(test)]
mod cache_ddl_tests {
    use super::*;

    fn req(stmt: &str, name: Option<&str>) -> CacheDDLRequest {
        CacheDDLRequest {
            unparsed_stmt: stmt.to_string(),
            schema_search_path: vec![],
            dialect: Dialect::DEFAULT_POSTGRESQL,
            cache_name: name.map(Relation::from),
        }
    }

    #[tokio::test]
    async fn add_cache_ddl_request_is_idempotent_by_name() {
        let authority = LocalAuthority::new();
        authority
            .add_cache_ddl_request(req("CREATE CACHE foo FROM SELECT 1", Some("foo")))
            .await
            .unwrap();
        authority
            .add_cache_ddl_request(req("CREATE CACHE foo FROM SELECT 1", Some("foo")))
            .await
            .unwrap();
        assert_eq!(authority.cache_ddl_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn remove_cache_ddl_requests_named_removes_match() {
        let authority = LocalAuthority::new();
        authority
            .add_cache_ddl_request(req("CREATE CACHE foo FROM SELECT 1", Some("foo")))
            .await
            .unwrap();
        let removed = authority
            .remove_cache_ddl_requests_named(&"foo".into())
            .await
            .unwrap();
        assert!(removed);
        assert!(authority.cache_ddl_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn remove_cache_ddl_requests_named_reports_no_match() {
        let authority = LocalAuthority::new();
        authority
            .add_cache_ddl_request(req("CREATE CACHE foo FROM SELECT 1", Some("foo")))
            .await
            .unwrap();
        let removed = authority
            .remove_cache_ddl_requests_named(&"bar".into())
            .await
            .unwrap();
        assert!(!removed);
        assert_eq!(authority.cache_ddl_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn shallow_remove_named_is_independent() {
        let authority = LocalAuthority::new();
        authority
            .add_shallow_cache_ddl_request(req("CREATE CACHE foo FROM SELECT 1", Some("foo")))
            .await
            .unwrap();
        authority
            .add_shallow_cache_ddl_request(req("CREATE CACHE foo FROM SELECT 1", Some("foo")))
            .await
            .unwrap();
        assert_eq!(
            authority.shallow_cache_ddl_requests().await.unwrap().len(),
            1
        );
        let removed = authority
            .remove_shallow_cache_ddl_requests_named(&"foo".into())
            .await
            .unwrap();
        assert!(removed);
        assert!(authority
            .shallow_cache_ddl_requests()
            .await
            .unwrap()
            .is_empty());
    }
}
