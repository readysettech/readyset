//! Collect information about the host we're running on.

use std::fmt::{Formatter, Result};
use std::thread::available_parallelism;
use std::{fmt::Display, path::Path};

use sysinfo::{Disks, MemoryRefreshKind, RefreshKind, System};

#[derive(Debug)]
pub struct HostInfo {
    pub cpus: usize,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub arch: &'static str,
    pub os: String,
    pub kernel: String,
    pub container: Container,
    pub numa_nodes: usize,
}

pub fn collect_host_info(disk_path: &Path) -> HostInfo {
    let system = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
    );
    let memory_bytes = system
        .cgroup_limits()
        .map_or_else(|| system.total_memory(), |limits| limits.total_memory);

    HostInfo {
        cpus: available_parallelism().map_or(0, |p| p.get()),
        memory_bytes,
        disk_bytes: disk_total_bytes(disk_path),
        arch: std::env::consts::ARCH,
        os: System::long_os_version().unwrap_or_else(|| std::env::consts::OS.to_owned()),
        kernel: System::kernel_version().unwrap_or_default(),
        container: Container::detect(),
        numa_nodes: numa_node_count(),
    }
}

fn disk_total_bytes(path: &Path) -> u64 {
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    Disks::new_with_refreshed_list()
        .iter()
        .filter(|disk| resolved.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len())
        .map_or(0, |disk| disk.total_space())
}

fn numa_node_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/sys/devices/system/node")
            .map(|dir| {
                dir.flatten()
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .and_then(|name| name.strip_prefix("node"))
                            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
                    })
                    .count()
            })
            .unwrap_or(1)
    }
    #[cfg(not(target_os = "linux"))]
    {
        1
    }
}

/// A container runtime or orchestrator we're running under, or [`Container::None`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Container {
    None,
    Kubernetes,
    Docker,
    Podman,
    Lxc,
}

impl Display for Container {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str(match self {
            Container::None => "none",
            Container::Kubernetes => "kubernetes",
            Container::Docker => "docker",
            Container::Podman => "podman",
            Container::Lxc => "lxc",
        })
    }
}

impl Container {
    /// Detect which container runtime we're running under, or [`Container::None`] if none.
    pub fn detect() -> Self {
        #[cfg(not(target_os = "linux"))]
        {
            Container::None
        }
        #[cfg(target_os = "linux")]
        {
            // Kubernetes injects this into every pod, so it's the most specific signal.
            if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() {
                return Container::Kubernetes;
            }
            if Path::new("/run/.containerenv").exists() {
                return Container::Podman;
            }
            if Path::new("/.dockerenv").exists() {
                return Container::Docker;
            }
            // The cgroup path usually carries the runtime name; use it as a fallback.
            std::fs::read_to_string("/proc/self/cgroup")
                .map(|cgroup| Self::from_cgroup(&cgroup))
                .unwrap_or(Container::None)
        }
    }

    /// Classify a `/proc/self/cgroup` payload by the runtime token in its path.
    #[cfg(target_os = "linux")]
    fn from_cgroup(contents: &str) -> Self {
        if contents.contains("kubepods") {
            Container::Kubernetes
        } else if contents.contains("libpod") {
            Container::Podman
        } else if contents.contains("docker") || contents.contains("containerd") {
            Container::Docker
        } else if contents.contains("/lxc") {
            Container::Lxc
        } else {
            Container::None
        }
    }
}
