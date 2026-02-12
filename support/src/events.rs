use clap::Parser;
use readyset_client::ReadySetHandle;
use tokio::sync::broadcast::error::RecvError;
use url::Url;

/// Watch SSE events from Readyset server
#[derive(Parser)]
pub(crate) struct Options {
    /// Readyset server URL (e.g., http://localhost:6033)
    #[arg(long, default_value = "http://localhost:6033")]
    url: String,
}

impl Options {
    pub(crate) async fn run(self) -> anyhow::Result<()> {
        tracing::info!(url = %self.url, "Connecting to Readyset server");

        let server_url = Url::parse(&self.url)?;
        let mut handle = ReadySetHandle::make_raw(server_url, None, None);
        let mut events_receiver = handle.subscribe_to_events();

        tracing::info!("Subscribed to events channel, waiting for events...");

        loop {
            match events_receiver.recv().await {
                Ok(event) => tracing::info!(?event, "Event received"),
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!(
                        skipped = n,
                        "Receiver lagged behind, some events were dropped"
                    );
                }
                Err(RecvError::Closed) => {
                    tracing::info!("Event channel closed");
                    break;
                }
            }
        }

        Ok(())
    }
}
