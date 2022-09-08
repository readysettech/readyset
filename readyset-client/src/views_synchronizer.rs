use readyset::{ControllerHandle, ReadySetResult, ViewCreateRequest};
use tokio::select;
use tracing::{info, instrument, warn};

use crate::query_status_cache::{MigrationState, QueryStatusCache};

pub struct ViewsSynchronizer {
    /// The noria connector used to query
    controller: ControllerHandle,
    /// The query status cache is updated according to which queries exist in noria
    query_status_cache: &'static QueryStatusCache,
    /// The interval between subsequent pollings of the Leader for migrated queries
    poll_interval: std::time::Duration,
    /// Receiver to return the shutdown signal on
    shutdown_recv: tokio::sync::broadcast::Receiver<()>,
}

impl ViewsSynchronizer {
    pub fn new(
        controller: ControllerHandle,
        query_status_cache: &'static QueryStatusCache,
        poll_interval: std::time::Duration,
        shutdown_recv: tokio::sync::broadcast::Receiver<()>,
    ) -> Self {
        ViewsSynchronizer {
            controller,
            query_status_cache,
            poll_interval,
            shutdown_recv,
        }
    }

    //TODO(DAN): add metrics on views synchronizer performance (e.g., number of queries polled,
    //time spent processing)
    #[instrument(level = "warn", name = "views_synchronizer", skip(self))]
    pub async fn run(&mut self) -> ReadySetResult<()> {
        let mut interval = tokio::time::interval(self.poll_interval);
        loop {
            select! {
                _ = interval.tick() => {
                    match self.controller.verbose_views().await {
                        Ok(views) => {
                            //TODO(Dan): Update so that we only request changes to output since
                            //some timestamp. Also consider using query hashes instead of SqlQuery
                            views.iter().for_each(|(_, (query, always))| {
                                let view_request = ViewCreateRequest::new(query.clone(), vec![]);
                                self.query_status_cache
                                    .update_query_migration_state(
                                        &view_request,
                                        MigrationState::Successful
                                    );
                                self.query_status_cache.always_attempt_readyset(&view_request, *always);
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "Could not get views from Leader");
                        }
                    }

                }
                _= self.shutdown_recv.recv() => {
                    info!("Views Synchronizer shutting down after shut down signal received");
                    break;
                }
            }
        }
        Ok(())
    }
}
