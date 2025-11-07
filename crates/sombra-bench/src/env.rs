#![forbid(unsafe_code)]

use std::path::Path;
use std::process::Command;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sysinfo::{Disks, RefreshKind, System};

#[derive(Debug, Serialize)]
pub struct EnvMetadata {
    pub timestamp_utc: DateTime<Utc>,
    pub hostname: Option<String>,
    pub os_version: Option<String>,
    pub kernel_version: Option<String>,
    pub cpu_brand: Option<String>,
    pub cpu_physical_cores: Option<usize>,
    pub cpu_logical_cores: usize,
    pub total_memory_bytes: u64,
    pub disk: Option<DiskMetadata>,
    pub git: Option<GitMetadata>,
}

#[derive(Debug, Serialize)]
pub struct DiskMetadata {
    pub mount_point: String,
    pub filesystem: Option<String>,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct GitMetadata {
    pub commit: String,
    pub branch: Option<String>,
    pub dirty: bool,
}

impl EnvMetadata {
    pub fn collect(result_dir: &Path) -> Self {
        let mut sys = System::new_with_specifics(RefreshKind::everything());
        sys.refresh_all();
        let timestamp_utc = Utc::now();
        let hostname = System::host_name();
        let os_version = System::long_os_version();
        let kernel_version = System::kernel_version();
        let cpu_brand = sys.cpus().get(0).map(|cpu| cpu.brand().to_string());
        let cpu_physical_cores = sys.physical_core_count();
        let cpu_logical_cores = sys.cpus().len().max(1);
        let total_memory_bytes = sys.total_memory() * 1024;
        let disk = find_disk(result_dir);
        let git = collect_git_metadata();
        Self {
            timestamp_utc,
            hostname,
            os_version,
            kernel_version,
            cpu_brand,
            cpu_physical_cores,
            cpu_logical_cores,
            total_memory_bytes,
            disk,
            git,
        }
    }
}

fn find_disk(path: &Path) -> Option<DiskMetadata> {
    let canonical = path.canonicalize().ok()?;
    let disks = Disks::new_with_refreshed_list();
    disks
        .list()
        .iter()
        .filter_map(|disk| {
            let mount = disk.mount_point();
            if canonical.starts_with(mount) {
                Some(DiskMetadata {
                    mount_point: mount.display().to_string(),
                    filesystem: Some(disk.file_system().to_string_lossy().into_owned()),
                    total_bytes: disk.total_space(),
                    available_bytes: disk.available_space(),
                })
            } else {
                None
            }
        })
        .max_by_key(|disk| disk.mount_point.len())
}

fn collect_git_metadata() -> Option<GitMetadata> {
    let commit = run_git(&["rev-parse", "HEAD"])?;
    let commit = commit.trim().to_string();
    let branch = run_git(&["rev-parse", "--abbrev-ref", "HEAD"]).map(|s| s.trim().to_string());
    let dirty = run_git(&["status", "--porcelain"])
        .map(|out| !out.trim().is_empty())
        .unwrap_or(false);
    Some(GitMetadata {
        commit,
        branch,
        dirty,
    })
}

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
