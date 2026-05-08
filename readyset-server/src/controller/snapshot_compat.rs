//! Backwards-compatibility snapshots for the `rmp_serde` positional encoding
//! that the Authority's RocksDB uses for `ControllerState`.
//!
//! Each named snapshot captures a wire shape that current and future builds
//! must still be able to decode. The compat test in
//! `tests/state_backwards_compatibility.rs` reads each fixture from
//! `tests/state_snapshots/<name>.bin` and verifies the current build decodes
//! it correctly via `rmp_serde::from_slice`, the exact codepath
//! `Authority::try_read_raw_controller_state` exercises in production. The
//! per-snapshot inventory and provenance live in
//! `tests/state_snapshots/README.md`.
//!
//! The `make_state_snapshot` binary in `readyset-tools` calls
//! [`build_snapshot_bytes`] to generate a new snapshot file the first time
//! a name appears in the snapshot set. Snapshots are immutable artifacts of
//! deployed state; if a wire shape changes, register a new snapshot name
//! rather than regenerating one in place.

// This module intentionally references deprecated compat-shim types
// (Sharding, Config.sharding, DfState.sharding, Node.sharded_by); that is
// the entire point of the snapshot fixtures. The `#[allow(deprecated)]`
// annotations stay narrow (use statement + per-fn) so unrelated
// deprecations would still surface as warnings here.

use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use dataflow::node::{self, Column};
use dataflow::prelude::ChannelCoordinator;
#[allow(deprecated)]
use dataflow::Sharding;
use readyset_data::{DfType, Dialect};

use super::sql::Recipe;
use super::state::DfState;
use super::ControllerState;
use crate::materialization::Materializations;
use crate::Config;

/// Every snapshot this build can construct or verify. Adding a new entry
/// here is the deliberate act that authorises generating a new
/// `<name>.bin` fixture.
pub const SNAPSHOT_NAMES: &[&str] = &["v1"];

/// Build the rmp_serde-encoded bytes for the named snapshot. The
/// representation matches exactly what the Authority writes to RocksDB.
pub fn build_snapshot_bytes(name: &str) -> Result<Vec<u8>> {
    let state = build_state(name)?;
    rmp_serde::to_vec(&state).context("rmp_serde encode")
}

/// Decode the bytes for the named snapshot via the production
/// `rmp_serde::from_slice` codepath and verify the sentinel field values
/// that define this snapshot. Returns `Err` if decode fails or any
/// sentinel mismatches.
pub fn verify_snapshot(name: &str, bytes: &[u8]) -> Result<()> {
    let state: ControllerState =
        rmp_serde::from_slice(bytes).with_context(|| format!("decoding snapshot `{name}`"))?;
    match name {
        "v1" => verify_v1(&state),
        other => bail!("unknown snapshot name `{other}`"),
    }
}

fn build_state(name: &str) -> Result<ControllerState> {
    match name {
        "v1" => Ok(build_v1_state()),
        other => bail!("unknown snapshot name `{other}`"),
    }
}

#[allow(deprecated)]
fn build_v1_state() -> ControllerState {
    // `v1` captures `ControllerState` with the legacy sharding fields
    // populated: `Config.sharding`, `DfState.sharding`, and a `Node` whose
    // `sharded_by` is non-`None`. The compat shims preserve the original
    // positional layout, so today's build produces the same bytes; see
    // `tests/state_snapshots/README.md` for provenance.
    let config = Config {
        sharding: Some(7),
        min_workers: 3,
        ..Config::default()
    };

    let mut g = petgraph::Graph::new();
    let source = g.add_node(node::Node::new::<_, _, Vec<Column>, _>(
        "source",
        Vec::new(),
        node::special::Source,
    ));
    let columns: Vec<Column> = vec![
        Column::new("id".into(), DfType::Int, None),
        Column::new("val".into(), DfType::Int, None),
    ];
    let mut base = node::Node::new(
        "t",
        columns,
        node::special::Base::new().with_primary_key([0]),
    );
    base.set_sharded_by_for_compat_test(Sharding::ByColumn(0, 2));
    g.add_node(base);

    let recipe = Recipe::with_config(
        Dialect::DEFAULT_POSTGRESQL,
        super::sql::Config::default(),
        super::sql::mir::Config::default(),
        false,
    );

    let mut state = DfState::new(
        g,
        source,
        0,
        config.domain_config.clone(),
        config.persistence.clone(),
        Materializations::new(),
        recipe,
        None,
        Arc::new(ChannelCoordinator::new()),
    );
    state.ndomains = 42;
    state.sharding = Some(7);

    ControllerState {
        config,
        dataflow_state: state,
    }
}

#[allow(deprecated)]
fn verify_v1(state: &ControllerState) -> Result<()> {
    let cfg_sharding = state.config.sharding;
    ensure!(
        cfg_sharding == Some(7),
        "Config.sharding sentinel: expected Some(7), got {cfg_sharding:?}"
    );
    ensure!(
        state.config.min_workers == 3,
        "Config.min_workers sentinel: expected 3, got {}",
        state.config.min_workers
    );
    ensure!(
        state.dataflow_state.ndomains == 42,
        "DfState.ndomains sentinel: expected 42, got {}",
        state.dataflow_state.ndomains
    );
    let df_sharding = state.dataflow_state.sharding;
    ensure!(
        df_sharding == Some(7),
        "DfState.sharding sentinel: expected Some(7), got {df_sharding:?}"
    );
    let target = Sharding::ByColumn(0, 2);
    let has_node = state
        .dataflow_state
        .ingredients
        .node_weights()
        .any(|n| n.sharded_by_for_compat_test() == target);
    ensure!(
        has_node,
        "Node.sharded_by sentinel: no node with Sharding::ByColumn(0, 2)"
    );
    Ok(())
}
