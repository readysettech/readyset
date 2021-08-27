/// Defintions of things specific to how Substrate organizes Terraform modules.
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use clap::Clap;

// Important note about region. Region here is not only an AWS region but also includes the special
// region "global" which represents things which are only in one place.

// There are different kind of root modules with different directory structures that Substrate generates
// This trait represents those different kind of root modules and what we can get from them.
pub(crate) trait ModuleLocator {
    // Where is this root module located in the repository
    fn to_terraform_path(&self) -> Result<PathBuf>;
    // A description of the root module
    fn to_description(&self) -> String;
    // What args need to be passed in to this command to reference this module
    fn to_args(&self) -> Vec<String>;
}

// The most common kind of root module is Service which contains all of the different forms
#[derive(Clap, Debug)]
pub(crate) struct ServiceModuleLocator {
    domain: String,
    environment: String,
    #[clap(long, default_value = "default")]
    quality: String,
    region: String,
}

// The admin modules contain IAMs and SSO configuration only. These only have quality and region.
#[derive(Clap, Debug)]
pub(crate) struct AdminModuleLocator {
    #[clap(long, default_value = "default")]
    quality: String,
    region: String,
}

impl ModuleLocator for ServiceModuleLocator {
    fn to_terraform_path(&self) -> Result<PathBuf> {
        let mut path: PathBuf = PathBuf::from("root-modules");
        if !path.is_dir() {
            bail!("Cannot find root modules path")
        }
        path.push(self.domain.clone());
        if !path.is_dir() {
            bail!("Cannot find domain root modules path")
        }
        path.push(self.environment.clone());
        if !path.is_dir() {
            bail!("Cannot find environment root modules path")
        }
        path.push(self.quality.clone());
        if !path.is_dir() {
            bail!("Cannot find quality root modules path")
        }
        path.push(self.region.clone());
        if !path.is_dir() {
            bail!("Cannot find region root module path")
        }
        Ok(path)
    }

    fn to_description(&self) -> String {
        format!(
            "Service Root Domain: {} Environment: {} Quality: {} Region: {}",
            self.domain, self.environment, self.quality, self.region
        )
    }

    fn to_args(&self) -> Vec<String> {
        vec![
            String::from("--quality"),
            self.quality.clone(),
            self.domain.clone(),
            self.environment.clone(),
            self.region.clone(),
        ]
    }
}

impl ModuleLocator for AdminModuleLocator {
    fn to_terraform_path(&self) -> Result<PathBuf> {
        let mut path: PathBuf = PathBuf::from("root-modules");
        if !path.is_dir() {
            bail!("Cannot find root modules path")
        }
        path.push("admin");
        if !path.is_dir() {
            bail!("Cannot find admin root modules path")
        }
        path.push(self.quality.clone());
        if !path.is_dir() {
            bail!("Cannot find quality root module path")
        }
        path.push(self.region.clone());
        if !path.is_dir() {
            bail!("Cannot find region root module path")
        }
        Ok(path)
    }

    fn to_description(&self) -> String {
        format!(
            "Admin Root Quality: {} Region: {}",
            self.quality, self.region
        )
    }

    fn to_args(&self) -> Vec<String> {
        vec![
            String::from("--quality"),
            self.quality.clone(),
            self.region.clone(),
        ]
    }
}

fn root_modules_path() -> PathBuf {
    Path::new("root-modules").to_path_buf()
}

// TODO: In one scan over the directory, generate all the different module locators. As such, this
// should eventually return Result<Vec<dyn ModuleLocator>>
pub(crate) fn find_all_admin_module_locators() -> Result<Vec<AdminModuleLocator>> {
    let mut admin_module_locators: Vec<AdminModuleLocator> = vec![];
    let admin_path = root_modules_path().join("admin");
    for entry in admin_path.read_dir()? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // If it is a directory, assume it is a quality.
            let quality = path
                .file_name()
                .map(std::ffi::OsStr::to_string_lossy)
                .unwrap()
                .to_string();
            for entry in path.read_dir()? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    // If it is a directory, assume it is a region.
                    let region = path
                        .file_name()
                        .map(std::ffi::OsStr::to_string_lossy)
                        .unwrap()
                        .to_string();
                    // If there is a main.tf, this is valid and we should add this to the list.
                    if path.join("main.tf").exists() {
                        admin_module_locators.push(AdminModuleLocator {
                            quality: String::from(&quality),
                            region: String::from(&region),
                        });
                    }
                }
            }
        }
    }
    Ok(admin_module_locators)
}

pub(crate) fn find_all_service_module_locators() -> Result<Vec<ServiceModuleLocator>> {
    let mut service_module_locators: Vec<ServiceModuleLocator> = vec![];
    // TODO: Supporting only the readyset/build domain/environment to get this started.
    // Fetch the list of domains/environments from `substrate-account -format json`
    let domain = String::from("readyset");
    let environment = String::from("build");
    let service_path = root_modules_path().join(&domain).join(&environment);
    for entry in service_path.read_dir()? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // If it is a directory, assume it is a quality.
            let quality = path
                .file_name()
                .map(std::ffi::OsStr::to_string_lossy)
                .unwrap()
                .to_string();
            for entry in path.read_dir()? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    // If it is a directory, assume it is a region.
                    let region = path
                        .file_name()
                        .map(std::ffi::OsStr::to_string_lossy)
                        .unwrap()
                        .to_string();
                    // If there is a main.tf, this is valid and we should add this to the list.
                    if path.join("main.tf").exists() {
                        service_module_locators.push(ServiceModuleLocator {
                            domain: String::from(&domain),
                            environment: String::from(&environment),
                            quality: String::from(&quality),
                            region: String::from(&region),
                        });
                    }
                }
            }
        }
    }
    Ok(service_module_locators)
}
