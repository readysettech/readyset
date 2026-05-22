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
// (Sharding, Config.sharding, DfState.sharding, Node.sharded_by,
// Config.worker_request_timeout); that is the entire point of the snapshot
// fixtures. The `#[allow(deprecated)]` annotations stay narrow (use statement
// + per-fn) so unrelated deprecations would still surface as warnings here.

use std::sync::Arc;
use std::time::Duration;

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
pub const SNAPSHOT_NAMES: &[&str] = &["v1", "v2", "v3"];

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
        "v2" => verify_v2(&state),
        "v3" => verify_v3(&state),
        other => bail!("unknown snapshot name `{other}`"),
    }
}

fn build_state(name: &str) -> Result<ControllerState> {
    match name {
        "v1" => Ok(build_v1_state()),
        "v2" => Ok(build_v2_state()),
        "v3" => Ok(build_v3_state()),
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

#[allow(deprecated)]
fn build_v2_state() -> ControllerState {
    // `v2` captures `ControllerState` with the fields that domain-replication
    // removal would otherwise have broken: `Config.min_workers`,
    // `Config.replication_strategy`, `DfState.replication_strategy`, and
    // `DfState.node_restrictions`, each populated to a non-default value. The
    // compat shims keep these at their original positions so older payloads
    // still decode.
    let config = Config {
        min_workers: 5,
        replication_strategy: super::replication::ReplicationStrategy::ReaderDomains(2),
        ..Config::default()
    };

    let mut state = minimal_dfstate();
    state.replication_strategy = super::replication::ReplicationStrategy::NonBaseDomains(4);
    state.node_restrictions.insert(
        super::NodeRestrictionKey::for_compat_test("t".into(), 0),
        super::DomainPlacementRestriction::for_compat_test(Some("vol-1".to_string())),
    );

    ControllerState {
        config,
        dataflow_state: state,
    }
}

#[allow(deprecated)]
fn verify_v2(state: &ControllerState) -> Result<()> {
    ensure!(
        state.config.min_workers == 5,
        "Config.min_workers sentinel: expected 5, got {}",
        state.config.min_workers
    );
    let cfg_strategy = state.config.replication_strategy;
    ensure!(
        cfg_strategy == super::replication::ReplicationStrategy::ReaderDomains(2),
        "Config.replication_strategy sentinel: expected ReaderDomains(2), got {cfg_strategy:?}"
    );
    let df_strategy = state.dataflow_state.replication_strategy;
    ensure!(
        df_strategy == super::replication::ReplicationStrategy::NonBaseDomains(4),
        "DfState.replication_strategy sentinel: expected NonBaseDomains(4), got {df_strategy:?}"
    );
    let key = super::NodeRestrictionKey::for_compat_test("t".into(), 0);
    let expected = super::DomainPlacementRestriction::for_compat_test(Some("vol-1".to_string()));
    let got = state.dataflow_state.node_restrictions.get(&key);
    ensure!(
        got == Some(&expected),
        "DfState.node_restrictions sentinel: expected {expected:?} at key {key:?}, got {got:?}"
    );
    Ok(())
}

#[allow(deprecated)]
fn build_v3_state() -> ControllerState {
    // `v3` captures `ControllerState` with `Config.worker_request_timeout` populated
    // to a sentinel value. The compat shim added in the prior commit keeps the
    // field at its original positional slot in `Config` so older payloads still
    // decode. `upquery_timeout` and `background_recovery_interval` are pinned to
    // explicit values (not derived via `..Config::default()`) so that
    // `verify_v3`'s neighbor assertions remain robust against legitimate tuning
    // of `Config::default()`.
    let config = Config {
        worker_request_timeout: Duration::from_secs(123),
        upquery_timeout: Duration::from_millis(5000),
        background_recovery_interval: Duration::from_secs(20),
        ..Config::default()
    };
    let dataflow_state = minimal_dfstate();
    ControllerState {
        config,
        dataflow_state,
    }
}

#[allow(deprecated)]
fn verify_v3(state: &ControllerState) -> Result<()> {
    let timeout = state.config.worker_request_timeout;
    ensure!(
        timeout == Duration::from_secs(123),
        "Config.worker_request_timeout sentinel: expected Duration::from_secs(123), got {timeout:?}"
    );
    // Neighbor defaults defend against a future refactor that re-orders the
    // adjacent `Duration` fields. If `upquery_timeout` and
    // `worker_request_timeout` swap positions, decoding silently lands the
    // sentinel in the wrong slot and the sibling assertion below trips first.
    let upquery = state.config.upquery_timeout;
    ensure!(
        upquery == Duration::from_millis(5000),
        "Config.upquery_timeout default: expected Duration::from_millis(5000), got {upquery:?}"
    );
    let recovery = state.config.background_recovery_interval;
    ensure!(
        recovery == Duration::from_secs(20),
        "Config.background_recovery_interval default: expected Duration::from_secs(20), got {recovery:?}"
    );
    Ok(())
}

/// Minimal `DfState` skeleton (Source + single-column `t` base, default Recipe,
/// empty `ChannelCoordinator`) shared by `build_v2_state` and `build_v3_state`.
/// `build_v1_state` builds inline because its `t` is two-column and carries
/// `Sharding::ByColumn(0, 2)` on the node.
fn minimal_dfstate() -> DfState {
    let defaults = Config::default();
    let mut g = petgraph::Graph::new();
    let source = g.add_node(node::Node::new::<_, _, Vec<Column>, _>(
        "source",
        Vec::new(),
        node::special::Source,
    ));
    let columns: Vec<Column> = vec![Column::new("id".into(), DfType::Int, None)];
    g.add_node(node::Node::new(
        "t",
        columns,
        node::special::Base::new().with_primary_key([0]),
    ));

    let recipe = Recipe::with_config(
        Dialect::DEFAULT_POSTGRESQL,
        super::sql::Config::default(),
        super::sql::mir::Config::default(),
        false,
    );

    DfState::new(
        g,
        source,
        0,
        defaults.domain_config,
        defaults.persistence,
        Materializations::new(),
        recipe,
        None,
        Arc::new(ChannelCoordinator::new()),
    )
}
