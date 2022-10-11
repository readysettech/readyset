use std::env;

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
    "\ncommit_id:      ",
    COMMIT_ID,
    "\nplatform:       ",
    PLATFORM,
    "\nrustc_version:  ",
    RUSTC_VERSION,
    "\nprofile:        ",
    PROFILE,
    "\nopt_level:      ",
    OPT_LEVEL,
);
