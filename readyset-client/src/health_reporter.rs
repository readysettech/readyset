use std::fmt::Display;
use std::sync::Arc;

use chrono::Utc;
use parking_lot::RwLock;

type TransitionTime = chrono::DateTime<Utc>;

/// Indicates the current adapter state along with a transition time. The transition time can be
/// used to infer how long the current state has persisted for.
#[derive(Clone, Copy)]
pub struct AdapterHealth {
    pub state: State,
    pub transition_time: TransitionTime,
}

impl AdapterHealth {
    fn new(state: State) -> AdapterHealth {
        AdapterHealth {
            state,
            transition_time: Utc::now(),
        }
    }
}

/// All known adapter states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Healthy,
    Unhealthy,
    ShuttingDown,
    Unknown,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            State::Healthy => "healthy",
            State::Unhealthy => "unhealthy",
            State::ShuttingDown => "shutting down",
            State::Unknown => "unknown",
        };
        write!(f, "{}", s)
    }
}

/// The AdapterHealthReporter can be used to record the current state of the adapter, and is
/// intended to also be used by the adapters http router to report the adapters health.
#[derive(Clone)]
pub struct AdapterHealthReporter {
    health: Arc<RwLock<AdapterHealth>>,
}

impl Default for AdapterHealthReporter {
    fn default() -> Self {
        AdapterHealthReporter::new()
    }
}

impl AdapterHealthReporter {
    /// Returns a new AdapterHealthReporter with the Initializing state set.
    pub fn new() -> AdapterHealthReporter {
        let health = AdapterHealth::new(State::Unhealthy);
        AdapterHealthReporter {
            health: Arc::new(RwLock::new(health)),
        }
    }

    /// Returns the current state of the HealthReporter.
    pub fn state(&self) -> State {
        self.health.read().state
    }

    /// Returns the current health, which includes both the state and the last transition time.
    /// Involves cloning.
    pub fn health(&self) -> AdapterHealth {
        *self.health.read()
    }

    /// Updates the state of the HealthReporter with the provided new state. If the current state
    /// is the same as the provided state, then no write operation occurs. If the new state
    /// facilitates a state transition, then the state is updated with a current timestamp
    /// indicating the transition time.
    pub fn set_state(&mut self, new_state: State) {
        {
            let health = self.health.read();
            if health.state == new_state {
                // We only want to update our health if we have a state transition.
                return;
            }
        }
        let new_health = AdapterHealth::new(new_state);
        *self.health.write() = new_health;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reporter_starts_with_unhealthy() {
        let reporter = AdapterHealthReporter::new();

        let got = reporter.state();
        assert_eq!(got, State::Unhealthy);
    }

    #[test]
    fn can_change_state() {
        let mut reporter = AdapterHealthReporter::new();

        reporter.set_state(State::Healthy);

        let got = reporter.state();
        assert_eq!(got, State::Healthy);
    }

    #[test]
    fn same_state_no_transition_change() {
        let mut reporter = AdapterHealthReporter::new();

        reporter.set_state(State::Healthy);

        let first = reporter.health().transition_time;

        // Now we set the same state again and validate we get the same transition time.
        reporter.set_state(State::Healthy);

        let second = reporter.health().transition_time;
        assert_eq!(first, second);
    }
}
