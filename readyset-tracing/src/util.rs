use std::time::{Duration, Instant};

use tokio::select;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::warn;

pub enum LogDelayTime {
    OnceAfter(Duration),
    Every(Duration),
}

/// Log the given message at warn-level after a period of time, either once or on an interval.
/// Returns a handle which can be used to cancel the log
pub fn warn_delay<S>(message: S, time: LogDelayTime) -> LogDelay
where
    String: From<S>,
{
    let (cancel, mut cancel_rx) = oneshot::channel();
    let message = String::from(message);
    let jh = tokio::spawn(async move {
        let start = Instant::now();
        let (dur, once) = match time {
            LogDelayTime::OnceAfter(dur) => (dur, true),
            LogDelayTime::Every(dur) => (dur, false),
        };

        loop {
            select! {
                _ = &mut cancel_rx => break,
                _ = sleep(dur) => {
                    warn!(total_time_secs = %start.elapsed().as_secs(), "{message}");
                    if once {
                        break;
                    }
                }
            }
        }
    });

    LogDelay { _jh: jh, cancel }
}

pub struct LogDelay {
    _jh: JoinHandle<()>,
    cancel: oneshot::Sender<()>,
}

impl LogDelay {
    /// Cancel the log delay, preventing any messages from being logged
    pub fn cancel(self) {
        let _ = self.cancel.send(());
    }
}
