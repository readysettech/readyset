use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use readyset_errors::{internal, ReadySetResult};
use tokio::sync::{mpsc::UnboundedReceiver, RwLock};
use tracing::error;

use readyset_client::{ControllerConnectionPool, TableStatus};
use readyset_sql::ast::{NonReplicatedRelation, Relation};
use readyset_util::shutdown::ShutdownReceiver;

use super::state::DfState;

const BATCH_SIZE: usize = 128;

/// A state machine maintaining the status of each table.
#[derive(Debug, Clone)]
pub struct TableStatusState {
    state: Arc<RwLock<Option<BTreeMap<Relation, TableStatus>>>>,
}

impl TableStatusState {
    /// Create the table status state machine and a tokio task that processes update sent via the
    /// returned channel.
    ///
    /// When running in single process mode, updates are sent directly to the local table status
    /// state machine to ensure that updates are ordered correctly.
    ///
    /// Wen running in distributed mode, updates are sent via the controller connection pool to the
    /// current leading controller.
    pub fn new(
        is_single_process: bool,
        mut controller_http: ControllerConnectionPool,
        mut table_status_rx: UnboundedReceiver<(Relation, TableStatus)>,
        shutdown_rx: ShutdownReceiver,
    ) -> Self {
        let table_statuses = Self {
            state: Arc::new(RwLock::new(None)),
        };
        let ret = table_statuses.clone();
        tokio::spawn(async move {
            let mut statuses = HashMap::new();
            loop {
                if shutdown_rx.signal_received() {
                    return;
                }

                // Wait for more status updates unless we are recovering from a previous loop.
                if statuses.is_empty() {
                    if let Some((table, status)) = table_status_rx.recv().await {
                        statuses.insert(table, status);
                    } else {
                        // The sender has closed.
                        return;
                    }
                }

                // Drain the contents of the incoming channel, up to the batch size.
                while let Ok((table, status)) = table_status_rx.try_recv() {
                    statuses.insert(table, status);
                    if statuses.len() >= BATCH_SIZE {
                        break;
                    }
                }

                if is_single_process {
                    // Update our local table status state machine.
                    if let Err(err) = table_statuses.set(statuses).await {
                        error!("Dropping table statuses: {err}");
                    }
                    statuses = HashMap::new();
                } else {
                    // The controller is not local to this process, send via HTTP.
                    let mut controller = match controller_http.use_connection().await {
                        Ok(controller) => controller,
                        Err(err) => {
                            error!("Failed to access controller connection pool: {err}");
                            continue;
                        }
                    };
                    if let Err(err) = controller.set_table_status(&statuses).await {
                        error!("Failed to send table statuses via HTTP: {err}");
                    } else {
                        statuses = HashMap::new();
                    }
                }
            }
        });
        ret
    }

    /// Initialize the table status state using the provided dataflow state.
    pub async fn init(&self, dataflow_state: &DfState) {
        let mut state = BTreeMap::new();
        for table in dataflow_state.tables().into_keys() {
            state.insert(table, TableStatus::Initializing);
        }
        for NonReplicatedRelation { name, reason } in dataflow_state.non_replicated_relations() {
            state.insert(name.clone(), TableStatus::NotReplicated(reason.clone()));
        }
        *self.state.write().await = Some(state);
    }

    /// Get all known table statuses.
    pub async fn get(&self, all: bool) -> BTreeMap<Relation, TableStatus> {
        let state = self.state.read().await;
        let state = match state.as_ref() {
            Some(state) => state,
            None => return BTreeMap::new(),
        };

        if all {
            state.clone()
        } else {
            state
                .iter()
                .filter_map(|(table, status)| {
                    if matches!(status, TableStatus::NotReplicated(..)) {
                        None
                    } else {
                        Some((table.clone(), status.clone()))
                    }
                })
                .collect()
        }
    }

    /// Update the statuses for multiple tables.
    ///
    /// NOTE: Only call this if you know what you're doing.  Most users should instead feed updates
    /// into the table_status_tx channel to ensure ordering is maintained.  This method instead
    /// inserts updates into the state machine now, possibly jumping ahead of ones in flight in the
    /// channel.
    pub async fn set(&self, statuses: HashMap<Relation, TableStatus>) -> ReadySetResult<()> {
        let mut state = self.state.write().await;
        let state = match &mut *state {
            Some(state) => state,
            None => internal!("Attempted to set table status when uninitialized."),
        };
        for (table, mut new_status) in statuses {
            if new_status == TableStatus::Dropped {
                state.remove(&table);
                continue;
            }
            let progress_update = match new_status {
                TableStatus::Snapshotting(Some(..))
                | TableStatus::Compacting(Some(..))
                | TableStatus::CreatingIndex(Some(..)) => true,
                TableStatus::Initializing
                | TableStatus::Snapshotting(None)
                | TableStatus::Compacting(None)
                | TableStatus::CreatingIndex(None)
                | TableStatus::Online
                | TableStatus::NotReplicated(..)
                | TableStatus::Dropped => false,
            };
            if let Some(status) = state.get_mut(&table) {
                if !progress_update {
                    *status = new_status;
                } else if status == &new_status {
                    // Progress updates only apply if they match the current status variant.
                    match new_status {
                        TableStatus::Snapshotting(Some(ref mut progress))
                        | TableStatus::Compacting(Some(ref mut progress))
                        | TableStatus::CreatingIndex(Some(ref mut progress)) => {
                            *progress = progress.clamp(0.0, 100.0);
                            *status = new_status;
                        }
                        TableStatus::Initializing
                        | TableStatus::Snapshotting(None)
                        | TableStatus::Compacting(None)
                        | TableStatus::CreatingIndex(None)
                        | TableStatus::Online
                        | TableStatus::NotReplicated(..)
                        | TableStatus::Dropped => {}
                    };
                }
            } else if !progress_update {
                // Ignore progress updates for unknown tables.
                state.insert(table, new_status);
            }
        }
        Ok(())
    }
}
