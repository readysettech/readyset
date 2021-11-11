#![feature(option_result_contains)]

use include_dir::{include_dir, Dir};
use noria_server::Config;
use std::ffi::OsStr;

const CONFIG_VERSIONS: Dir = include_dir!("tests/config_versions");

#[test]
fn deserialize_old_versions() {
    for file in CONFIG_VERSIONS.files() {
        if !file.path().extension().contains(&OsStr::new("json")) {
            continue;
        }

        let name = file.path().file_name().unwrap();
        let res = serde_json::from_slice::<Config>(file.contents);
        assert!(
            res.is_ok(),
            "Failed to deserialize {}: {:#}",
            name.to_str().unwrap(),
            res.err().unwrap()
        );
    }
}
