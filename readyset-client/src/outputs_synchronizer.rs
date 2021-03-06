use readyset::{ControllerHandle, ReadySetResult};
use tokio::select;
use tracing::{info, instrument, warn};

use crate::query_status_cache::{MigrationState, QueryStatusCache};

pub struct OutputsSynchronizer {
    /// The ReadySet connector used to query
    controller: ControllerHandle,
    /// The query status cache is updated according to which queries exist in ReadySet
    query_status_cache: &'static QueryStatusCache,
    /// The interval between subsequent pollings of the Leader for migrated queries
    poll_interval: std::time::Duration,
    /// Receiver to return the shutdown signal on
    shutdown_recv: tokio::sync::broadcast::Receiver<()>,
}

impl OutputsSynchronizer {
    pub fn new(
        controller: ControllerHandle,
        query_status_cache: &'static QueryStatusCache,
        poll_interval: std::time::Duration,
        shutdown_recv: tokio::sync::broadcast::Receiver<()>,
    ) -> Self {
        OutputsSynchronizer {
            controller,
            query_status_cache,
            poll_interval,
            shutdown_recv,
        }
    }

    //TODO(DAN): add metrics on outputs synchronizer performance (e.g., number of queries polled,
    //time spent processing)
    #[instrument(level = "warn", name = "outputs_synchronizer", skip(self))]
    pub async fn run(&mut self) -> ReadySetResult<()> {
        let mut interval = tokio::time::interval(self.poll_interval);
        loop {
            select! {
                _ = interval.tick() => {
                    match self.controller.verbose_outputs().await {
                        Ok(outputs) => {
                            //TODO(Dan): Update so that we only request changes to output since
                            //some timestamp. Also consider using query hashes instead of SqlQuery
                            outputs.iter().for_each(|(_, query)| {
                                self.query_status_cache
                                    .update_query_migration_state(query, MigrationState::Successful)
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Could not get outputs from Leader");
                        }
                    }

                }
                _= self.shutdown_recv.recv() => {
                    info!("Outputs Synchronizer shutting down after shut down signal received");
                    break;
                }
            }
        }
        Ok(())
    }
}
