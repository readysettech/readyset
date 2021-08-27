use std::{
    env,
    fs::{self, File},
    io::BufReader,
    os::unix::prelude::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Result};
use clap::{AppSettings, Clap};
use serde_json::{json, map::Entry};
use sha2::Digest;
use tracing::{event, Level};
use tracing_subscriber::EnvFilter;

#[derive(Clap, Debug)]
enum Subcommand {
    InstallDockerCredentialECRLogin,
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

/// Translate the Rust's env::consts::OS into the GOOS environment variable used for Go cross compiling
fn goos() -> &'static str {
    env::consts::OS
}

/// Translate the Rust env::consts::ARCH into the GOARCH environment variable used for Go cross compiling
fn goarch() -> &'static str {
    match env::consts::ARCH {
        "x86_64" => "amd64",
        arch => arch,
    }
}

fn install_docker_credential_ecr_login_binary_to_path(executable_dir: Option<&Path>) -> Result<()> {
    let binary_url = format!("https://amazon-ecr-credential-helper-releases.s3.us-east-2.amazonaws.com/0.5.0/{goos}-{goarch}/docker-credential-ecr-login", goos=goos(), goarch=goarch());

    if let Some(executable_dir) = executable_dir {
        let binary_sh256_url = format!("{}.sha256", binary_url);
        let source_sha256sum = reqwest::blocking::get(binary_sh256_url)?.text()?;
        let binary = reqwest::blocking::get(binary_url)?.bytes()?;
        let binary_sha256sum = format!(
            "{:x}  docker-credential-ecr-login\n",
            sha2::Sha256::digest(&binary)
        );
        if binary_sha256sum == source_sha256sum {
            let executable = executable_dir.join("docker-credential-ecr-login");
            fs::write(&executable, &binary)?;
            fs::set_permissions(&executable.as_path(), PermissionsExt::from_mode(0o755))?;
            event!(
                Level::INFO,
                "Installed docker-credential-ecr-login to {}. Make sure this is on your PATH.",
                executable_dir.display()
            );
        } else {
            bail!("Fetched binary did not match fetched SHA256. Something weird is going on.")
        }
    } else {
        event!(
            Level::ERROR,
            "Could not install docker-credential-ecr-login. Download {} and put it on your PATH.",
            binary_url
        );
    }
    Ok(())
}

fn install_docker_credential_ecr_login() -> Result<()> {
    let base_dirs =
        directories::BaseDirs::new().ok_or_else(|| anyhow!("Could not load base directories"))?;
    if which::which("docker-credential-ecr-login").is_err() {
        event!(
            Level::INFO,
            "Did not find docker-credential-ecr-login on your PATH. Attempting to install it."
        );
        install_docker_credential_ecr_login_binary_to_path(base_dirs.executable_dir())?;
    }

    let home_dir = base_dirs.home_dir();
    let docker_config_path = home_dir.join(".docker").join("config.json");
    let mut docker_config_modified = false;
    let mut docker_config: serde_json::Value = match File::open(&docker_config_path) {
        Ok(file) => serde_json::from_reader(BufReader::new(file))?,
        Err(_) => {
            docker_config_modified = true;
            json!({})
        }
    };

    let cred_helpers = docker_config
        .as_object_mut()
        .unwrap()
        .entry("credHelpers")
        .or_insert(json!({}))
        .as_object_mut()
        .unwrap();

    for ecr_registry in [
        "public.ecr.aws",
        "305232526136.dkr.ecr.us-east-2.amazonaws.com",
    ] {
        match cred_helpers.entry(ecr_registry) {
            Entry::Occupied(occupied) if (occupied.get().as_str() == Some("ecr-login")) => {}
            entry => {
                entry.or_insert(json!("ecr-login"));
                docker_config_modified = true;
            }
        }
    }

    if docker_config_modified {
        event!(
            Level::INFO,
            "Updated ~/.docker/config.json to use docker-credential-ecr-login."
        );
        fs::create_dir_all(docker_config_path.parent().unwrap())?;
        fs::write(
            &docker_config_path,
            serde_json::to_string_pretty(&docker_config)?,
        )?
    }

    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .without_time()
        .with_env_filter(EnvFilter::from_default_env().add_directive("xtask=info".parse()?))
        .init();

    let opts = Opts::parse();
    match opts.subcommand {
        Subcommand::InstallCommitMsgHook => install_commit_msg_hook(),
        Subcommand::InstallDockerCredentialECRLogin => install_docker_credential_ecr_login(),
    }
}
