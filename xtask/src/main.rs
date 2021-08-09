use std::{
    env, fs,
    os::unix::prelude::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::{AppSettings, Clap};

#[derive(Clap, Debug)]
enum Subcommand {
    InstallCommitMsgHook,
}

#[derive(Clap, Debug)]
#[clap(setting=AppSettings::SubcommandRequired)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

fn project_root_path() -> PathBuf {
    // This tries to pull the `CARGO_MANIFEST_DIR` from the environment. But we might also compile an
    // xtask to be used as say a pre-commit hook and `env!` will compile the location of the project
    // into the binary as `env!` is resolved at compile time.
    Path::new(
        &env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned()),
    )
    .ancestors()
    .nth(1)
    .unwrap()
    .to_path_buf()
}

fn install_commit_msg_hook() -> Result<()> {
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

fn main() -> Result<()> {
    let opts = Opts::parse();
    match opts.subcommand {
        Subcommand::InstallCommitMsgHook => install_commit_msg_hook(),
    }
}
