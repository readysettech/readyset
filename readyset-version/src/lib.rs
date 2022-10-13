//! Version Helpers
//!
//! ReadySetVersion allows the ReadySet version information to be displayed from the `SHOW READYSET
//! VERSION` SQL query.
//!
//! Also included in this modue are helper constants for use in logs (VERSION_STR_ONELINE) or
//! returning from --version (VERSION_STR_PRETTY)

use std::env;

use serde::{Deserialize, Serialize};

pub const COMMIT_ID: &str = env!(
    "BUILDKITE_COMMIT",
    "BUILDKITE_COMMIT should always be set, see build.rs"
);

pub const RELEASE_VERSION: &str = env!(
    "RELEASE_VERSION",
    "RELEASE_VERSION should always be set, see build.rs"
);

pub const PLATFORM: &str = env!("PLATFORM", "PLATFORM should always be set, see build.rs");
pub const RUSTC_VERSION: &str = env!(
    "RUSTC_VERSION",
    "RUSTC_VERSION should always be set, see build.rs"
);
pub const PROFILE: &str = env!("PROFILE", "PROFILE should always be set, see build.rs");
pub const OPT_LEVEL: &str = env!("OPT_LEVEL", "OPT_LEVEL should always be set, see build.rs");

pub const VERSION_STR_ONELINE: &str = const_str::concat!(
    "{\
release-version: ",
    RELEASE_VERSION,
    ", commit_id: ",
    COMMIT_ID,
    ", platform: ",
    PLATFORM,
    ", rustc_version: ",
    RUSTC_VERSION,
    ", profile: ",
    PROFILE,
    ", opt_level: ",
    OPT_LEVEL,
    "}"
);

// The first newline is because clap --version adds the binary version at the beginning
pub const VERSION_STR_PRETTY: &str = const_str::concat!(
    "
release-version: ",
    RELEASE_VERSION,
    "\ncommit_id:       ",
    COMMIT_ID,
    "\nplatform:        ",
    PLATFORM,
    "\nrustc_version:   ",
    RUSTC_VERSION,
    "\nprofile:         ",
    PROFILE,
    "\nopt_level:       ",
    OPT_LEVEL,
);

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ReadySetVersion {
    pub release_version: &'static str,
    pub commit_id: &'static str,
    pub platform: &'static str,
    pub rustc_version: &'static str,
    pub profile: &'static str,
    pub opt_level: &'static str,
}

// TODO(luke): We can probably implement Display/to_pretty_string() for this and do away with the
// consts above
pub static READYSET_VERSION: ReadySetVersion = ReadySetVersion {
    release_version: RELEASE_VERSION,
    commit_id: COMMIT_ID,
    platform: PLATFORM,
    rustc_version: RUSTC_VERSION,
    profile: PROFILE,
    opt_level: OPT_LEVEL,
};

impl From<ReadySetVersion> for Vec<(String, String)> {
    fn from(version: ReadySetVersion) -> Vec<(String, String)> {
        vec![
            ("ReadySet".to_string(), "Version Information".to_string()),
            (
                "release version".to_string(),
                version.release_version.to_string(),
            ),
            ("commit id".to_string(), version.commit_id.to_string()),
            ("platform".to_string(), version.platform.to_string()),
            (
                "rustc version".to_string(),
                version.rustc_version.to_string(),
            ),
            ("profile".to_string(), version.profile.to_string()),
            (
                "optimization level".to_string(),
                version.opt_level.to_string(),
            ),
        ]
    }
}
