//! Trait for interacting with an conensus system (Consul, etcd) to determine
//! which ReadySet worker acts as the controller, which ReadySet workers exist, detecting failed
//! workers which necessitate changes, and storing cluster wide global state.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::anyhow;
use async_trait::async_trait;
use clap::ValueEnum;
use enum_dispatch::enum_dispatch;
use nom_sql::SqlIdentifier;
use readyset_data::Dialect;
use readyset_errors::{ReadySetError, ReadySetResult};
use replication_offset::ReplicationOffset;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::error;
use url::Url;

mod consul;
mod local;
mod standalone;

pub use self::consul::ConsulAuthority;
pub use self::local::{LocalAuthority, LocalAuthorityStore};
pub use self::standalone::StandaloneAuthority;
use crate::debug::stats::PersistentStats;
use crate::ControllerDescriptor;

// This should be an associated type on Authority but since Authority will only have one possible
// type inside of ReadySet, we are using a type alias here instead.
// If Authority ever moves out of ReadySet, it should become an associated type.
// LeaderPayload must be Serialize + DeserializeOwned + PartialEq
type LeaderPayload = ControllerDescriptor;

pub type VolumeId = String;
pub type WorkerId = String;

const CACHE_DDL_REQUESTS_PATH: &str = "cache_ddl_requests";
const PERSISTENT_STATS_PATH: &str = "persistent_stats";
const SCHEMA_REPLICATION_OFFSET_PATH: &str = "schema_replication_offset";

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct CacheDDLRequest {
    pub unparsed_stmt: String,
    pub schema_search_path: Vec<SqlIdentifier>,
    pub dialect: Dialect,
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
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum NodeTypeSchedulingRestriction {
    /// No restrictions: all domains with or without this node type may be scheduled onto this
    /// worker
    None,
    /// Only domains containing this node type may be scheduled onto this worker
    OnlyWithNodeType,
    /// Domains containing this node type may never be scheduled onto this worker
    NeverWithNodeType,
}

impl Default for NodeTypeSchedulingRestriction {
    fn default() -> Self {
        Self::None
    }
}

/// Configuration for how domains should be scheduled onto a particular worker.
///
/// The [`Default`] value for this struct allows any domain to be scheduled onto any worker.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct WorkerSchedulingConfig {
    /// Identifier for the persistent volume associated with this worker, if any. This is used to
    /// make sure that once a domain with a particular base table is scheduled onto a worker, that
    /// domain will always be scheduled onto a worker with the same persistent volume.
    pub volume_id: Option<VolumeId>,
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

    /// Whether the authority can place a watch that can unpark the thread when
    /// there is a change in authority state.
    fn can_watch(&self) -> bool;

    /// Waits for a change in the leader, and returns on a change. This should only
    /// be called if `can_watch` returns True.
    async fn watch_leader(&self) -> ReadySetResult<()>;

    /// Waits for a change in the workers, and returns on a change. This should only
    /// be called if `can_watch` returns True.
    async fn watch_workers(&self) -> ReadySetResult<()>;

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
    async fn update_controller_state<F, U, P: 'static, E>(
        &self,
        f: F,
        u: U,
    ) -> ReadySetResult<Result<P, E>>
    where
        F: Send + FnMut(Option<P>) -> Result<P, E>,
        U: Send + FnMut(&mut P),
        P: Send + Serialize + DeserializeOwned + Clone,
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
        Ok(self.try_read::<Vec<String>>(CACHE_DDL_REQUESTS_PATH)
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
        let cache_ddl_req = serde_json::ser::to_string(&cache_ddl_req)?;
        modify_cache_ddl_requests(self, move |stmts| {
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

    /// Removes all stored cache ddl requests
    async fn remove_all_cache_ddl_requests(&self) -> ReadySetResult<()> {
        modify_cache_ddl_requests(self, move |stmts| {
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
            .flatten()
    }

    /// Returns the stored schema [`ReplicationOffset`], if present. Wrapper around Self::try_read.
    async fn schema_replication_offset(&self) -> ReadySetResult<Option<ReplicationOffset>> {
        self.try_read(SCHEMA_REPLICATION_OFFSET_PATH).await
    }

    /// Update the schema_replication_offset to the given value, without regard for what was stored
    /// there before.
    async fn set_schema_replication_offset<R>(
        &self,
        offset: Option<ReplicationOffset>,
    ) -> ReadySetResult<()>;
}

async fn modify_cache_ddl_requests<A, F>(authority: &A, mut f: F) -> ReadySetResult<()>
where
    A: AuthorityControl + ?Sized,
    F: FnMut(&mut Vec<String>) + Send,
{
    authority
        .read_modify_write::<_, Vec<String>, ReadySetError>(CACHE_DDL_REQUESTS_PATH, move |stmts| {
            let mut stmts = stmts.unwrap_or_default();
            f(&mut stmts);
            Ok(stmts)
        })
        .await??;

    Ok(())
}

/// Enum that dispatches calls to the `AuthorityControl` trait to
/// the respective variant.
#[allow(clippy::large_enum_variant)]
#[enum_dispatch(AuthorityControl)]
pub enum Authority {
    ConsulAuthority,
    LocalAuthority,
    StandaloneAuthority,
}

/// Enum that mirrors Authority that parses command line arguments.
#[derive(Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum AuthorityType {
    Consul,
    Local,
    Standalone,
}

impl FromStr for AuthorityType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "consul" => Ok(AuthorityType::Consul),
            "local" => Ok(AuthorityType::Local),
            "standalone" => Ok(AuthorityType::Standalone),
            other => Err(anyhow!("Invalid authority type: {}", other)),
        }
    }
}

impl Display for AuthorityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            AuthorityType::Consul => write!(f, "consul"),
            AuthorityType::Local => write!(f, "local"),
            AuthorityType::Standalone => write!(f, "standalone"),
        }
    }
}

impl AuthorityType {
    pub fn to_authority(&self, addr: &str, deployment: &str) -> Authority {
        match self {
            AuthorityType::Consul => Authority::from(
                ConsulAuthority::new(&format!("http://{}/{}", addr, deployment)).unwrap(),
            ),
            AuthorityType::Local => Authority::from(LocalAuthority::new()),
            AuthorityType::Standalone => {
                Authority::from(StandaloneAuthority::new(addr, deployment).unwrap())
            }
        }
    }
}

/// A wrapper around a gzip compressor
pub(crate) struct Compressor(cloudflare_zlib::Deflate);

impl Compressor {
    fn new() -> Self {
        Compressor(
            cloudflare_zlib::Deflate::new(6, cloudflare_zlib::Z_DEFAULT_STRATEGY, 15)
                .expect("Can't fail with valid params"),
        )
    }

    pub(crate) fn compress(data: &[u8]) -> Vec<u8> {
        let mut comp = Self::new();
        comp.0.compress(data).expect("Can't fail");
        comp.0.finish().expect("Can't fail")
    }
}
