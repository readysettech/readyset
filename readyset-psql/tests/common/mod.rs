use std::sync::Arc;

use readyset_client::consensus::{Authority, StandaloneAuthority};
use readyset_client_test_helpers::TestBuilder;
use readyset_server::DurabilityMode;
use tempfile::TempDir;

// This is used in integration.rs, but for some reason clippy isn't detecting that.
#[allow(dead_code)]
pub fn setup_standalone_with_authority(
    prefix: &str,
    dir: Option<TempDir>,
) -> (TestBuilder, Arc<Authority>, TempDir) {
    // Since we will be using DurabilityMode::Permanent, we return this tempdir so that it is
    // cleaned up after the outer test finishes
    let dir = dir.unwrap_or_else(|| tempfile::tempdir().unwrap());
    let dir_path = dir.path().to_str().unwrap();
    let authority = Arc::new(Authority::from(
        StandaloneAuthority::new(dir_path, prefix).unwrap(),
    ));
    let builder = TestBuilder::default()
        .replicate(false)
        .durability_mode(DurabilityMode::Permanent)
        .storage_dir_path(dir.path().into())
        .authority(authority.clone());

    (builder, authority, dir)
}
