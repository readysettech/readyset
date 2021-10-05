use std::fs;
use std::os::unix::prelude::PermissionsExt;

use anyhow::Result;

use crate::project_root_path;

pub fn run() -> Result<()> {
    let git_hooks_path = project_root_path().join(".git/hooks");
    let commmit_msg_hook_path = git_hooks_path.join("commit-msg");
    if commmit_msg_hook_path.exists() {
        println!("commit-msg hook already installed");
    } else {
        let hook_script =
            reqwest::blocking::get("https://gerrit.readyset.name/tools/hooks/commit-msg")?
                .text()?;
        fs::write(&commmit_msg_hook_path, hook_script)?;
        fs::set_permissions(
            &commmit_msg_hook_path.as_path(),
            PermissionsExt::from_mode(0o755),
        )?;
    }
    Ok(())
}
