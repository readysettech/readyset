use clap::Parser;
use readyset_client::ReadySetHandle;
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
                Err(error) => {
                    tracing::warn!(%error, "Error receiving event");
                    break;
                }
            }
        }

        Ok(())
    }
}
