use super::{SynchronousArg, VersionCodecArg};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Profile {
    pub name: String,
    pub database: Option<PathBuf>,
    pub page_size: Option<u32>,
    pub cache_pages: Option<usize>,
    pub synchronous: Option<SynchronousArg>,
    pub distinct_neighbors_default: Option<bool>,
    pub group_commit_max_writers: Option<usize>,
    pub group_commit_max_frames: Option<usize>,
    pub group_commit_max_wait_ms: Option<u64>,
    pub async_fsync: Option<bool>,
    pub async_fsync_max_wait_ms: Option<u64>,
    pub wal_segment_size_bytes: Option<u64>,
    pub wal_preallocate_segments: Option<u32>,
    pub inline_history: Option<bool>,
    pub inline_history_max_bytes: Option<usize>,
    pub version_codec: Option<VersionCodecArg>,
    pub version_codec_min_bytes: Option<usize>,
    pub version_codec_min_savings_bytes: Option<usize>,
    pub snapshot_pool_size: Option<usize>,
    pub snapshot_pool_max_age_ms: Option<u64>,
}

#[derive(Debug, Default)]
pub struct CliConfig {
    path: Option<PathBuf>,
    data: RawConfig,
    profiles: HashMap<String, Profile>,
}

impl CliConfig {
    pub fn load(explicit: Option<PathBuf>) -> Result<Self, ConfigError> {
        let path = explicit.or_else(default_config_path);
        let data = if let Some(config_path) = path.as_ref() {
            if config_path.exists() {
                read_file(config_path)?
            } else {
                RawConfig::default()
            }
        } else {
            RawConfig::default()
        };
        let profiles = parse_profiles(&data)?;
        Ok(Self {
            path,
            data,
            profiles,
        })
    }

    pub fn default_db_path(&self) -> Option<&PathBuf> {
        self.data.database.default_path.as_ref()
    }

    pub fn default_profile_name(&self) -> Option<&str> {
        self.data
            .default_profile
            .as_deref()
            .filter(|name| self.profiles.contains_key(*name))
    }

    pub fn profile(&self, name: &str) -> Option<&Profile> {
        self.profiles.get(name)
    }

    pub fn profiles(&self) -> impl Iterator<Item = &Profile> {
        self.profiles.values()
    }

    pub fn set_default_profile(&mut self, name: Option<&str>) -> Result<(), ConfigError> {
        if let Some(name) = name {
            if !self.profiles.contains_key(name) {
                return Err(ConfigError::ProfileNotFound {
                    name: name.to_string(),
                });
            }
            self.data.default_profile = Some(name.to_string());
        } else {
            self.data.default_profile = None;
        }
        Ok(())
    }

    pub fn upsert_profile(&mut self, name: &str, update: ProfileUpdate) -> Result<(), ConfigError> {
        let entry = self.data.profiles.entry(name.to_string()).or_default();
        if let Some(db) = update.database {
            entry.database = Some(db);
        }
        if let Some(page_size) = update.page_size {
            entry.page_size = Some(page_size);
        }
        if let Some(cache_pages) = update.cache_pages {
            entry.cache_pages = Some(cache_pages);
        }
        if let Some(sync) = update.synchronous {
            entry.synchronous = Some(sync_to_string(sync));
        }
        if let Some(max_writers) = update.group_commit_max_writers {
            entry.group_commit_max_writers = Some(max_writers);
        }
        if let Some(max_frames) = update.group_commit_max_frames {
            entry.group_commit_max_frames = Some(max_frames);
        }
        if let Some(wait_ms) = update.group_commit_max_wait_ms {
            entry.group_commit_max_wait_ms = Some(wait_ms);
        }
        if let Some(async_fsync) = update.async_fsync {
            entry.async_fsync = Some(async_fsync);
        }
        if let Some(async_wait) = update.async_fsync_max_wait_ms {
            entry.async_fsync_max_wait_ms = Some(async_wait);
        }
        if let Some(segment_bytes) = update.wal_segment_size_bytes {
            entry.wal_segment_size_bytes = Some(segment_bytes);
        }
        if let Some(preallocate) = update.wal_preallocate_segments {
            entry.wal_preallocate_segments = Some(preallocate);
        }
        if let Some(inline_history) = update.inline_history {
            entry.inline_history = Some(inline_history);
        }
        if let Some(max_bytes) = update.inline_history_max_bytes {
            entry.inline_history_max_bytes = Some(max_bytes);
        }
        if let Some(codec) = update.version_codec {
            entry.version_codec = Some(codec_to_string(codec));
        }
        if let Some(min_bytes) = update.version_codec_min_bytes {
            entry.version_codec_min_bytes = Some(min_bytes);
        }
        if let Some(min_savings) = update.version_codec_min_savings_bytes {
            entry.version_codec_min_savings_bytes = Some(min_savings);
        }
        if let Some(pool_size) = update.snapshot_pool_size {
            entry.snapshot_pool_size = Some(pool_size);
        }
        if let Some(pool_age) = update.snapshot_pool_max_age_ms {
            entry.snapshot_pool_max_age_ms = Some(pool_age);
        }
        if let Some(distinct) = update.distinct_neighbors_default {
            entry.distinct_neighbors_default = Some(distinct);
        }
        self.profiles = parse_profiles(&self.data)?;
        Ok(())
    }

    pub fn persist(&self) -> Result<PathBuf, ConfigError> {
        let target = if let Some(path) = &self.path {
            path.clone()
        } else if let Some(default) = default_config_path() {
            default
        } else {
            return Err(ConfigError::NoConfigPath);
        };
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|source| ConfigError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let serialized =
            toml::to_string_pretty(&self.data).map_err(|source| ConfigError::Serialize {
                source: Box::new(source),
            })?;
        fs::write(&target, serialized).map_err(|source| ConfigError::Write {
            path: target.clone(),
            source,
        })?;
        Ok(target)
    }

    pub fn delete_profile(&mut self, name: &str) -> Result<(), ConfigError> {
        if self.data.profiles.remove(name).is_none() {
            return Err(ConfigError::ProfileNotFound {
                name: name.to_string(),
            });
        }
        if self.data.default_profile.as_deref() == Some(name) {
            self.data.default_profile = None;
        }
        self.profiles = parse_profiles(&self.data)?;
        Ok(())
    }
}

fn sync_to_string(value: SynchronousArg) -> String {
    match value {
        SynchronousArg::Full => "full",
        SynchronousArg::Normal => "normal",
        SynchronousArg::Off => "off",
    }
    .to_string()
}

fn codec_to_string(value: VersionCodecArg) -> String {
    match value {
        VersionCodecArg::None => "none",
        VersionCodecArg::Snappy => "snappy",
    }
    .to_string()
}

fn read_file(path: &Path) -> Result<RawConfig, ConfigError> {
    let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&contents).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source: Box::new(source),
    })
}

fn parse_profiles(data: &RawConfig) -> Result<HashMap<String, Profile>, ConfigError> {
    let mut profiles = HashMap::new();
    for (name, raw) in &data.profiles {
        profiles.insert(name.clone(), convert_profile(name, raw)?);
    }
    if let Some(default_name) = data.default_profile.as_ref() {
        if !profiles.contains_key(default_name) {
            return Err(ConfigError::ProfileNotFound {
                name: default_name.clone(),
            });
        }
    }
    Ok(profiles)
}

fn convert_profile(name: &str, raw: &RawProfile) -> Result<Profile, ConfigError> {
    let synchronous = match raw.synchronous.as_deref() {
        Some(value) => Some(SynchronousArg::from_str(value, true).map_err(|_| {
            ConfigError::InvalidSynchronous {
                profile: name.to_string(),
                value: value.to_string(),
            }
        })?),
        None => None,
    };
    let version_codec = match raw.version_codec.as_deref() {
        Some(value) => Some(VersionCodecArg::from_str(value, true).map_err(|_| {
            ConfigError::InvalidVersionCodec {
                profile: name.to_string(),
                value: value.to_string(),
            }
        })?),
        None => None,
    };
    Ok(Profile {
        name: name.to_string(),
        database: raw.database.clone(),
        page_size: raw.page_size,
        cache_pages: raw.cache_pages,
        synchronous,
        distinct_neighbors_default: raw.distinct_neighbors_default,
        group_commit_max_writers: raw.group_commit_max_writers,
        group_commit_max_frames: raw.group_commit_max_frames,
        group_commit_max_wait_ms: raw.group_commit_max_wait_ms,
        async_fsync: raw.async_fsync,
        async_fsync_max_wait_ms: raw.async_fsync_max_wait_ms,
        wal_segment_size_bytes: raw.wal_segment_size_bytes,
        wal_preallocate_segments: raw.wal_preallocate_segments,
        inline_history: raw.inline_history,
        inline_history_max_bytes: raw.inline_history_max_bytes,
        version_codec,
        version_codec_min_bytes: raw.version_codec_min_bytes,
        version_codec_min_savings_bytes: raw.version_codec_min_savings_bytes,
        snapshot_pool_size: raw.snapshot_pool_size,
        snapshot_pool_max_age_ms: raw.snapshot_pool_max_age_ms,
    })
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RawConfig {
    #[serde(default)]
    database: DatabaseSection,
    #[serde(default)]
    profiles: HashMap<String, RawProfile>,
    #[serde(default)]
    default_profile: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct DatabaseSection {
    #[serde(rename = "default")]
    default_path: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RawProfile {
    database: Option<PathBuf>,
    page_size: Option<u32>,
    cache_pages: Option<usize>,
    synchronous: Option<String>,
    distinct_neighbors_default: Option<bool>,
    group_commit_max_writers: Option<usize>,
    group_commit_max_frames: Option<usize>,
    group_commit_max_wait_ms: Option<u64>,
    async_fsync: Option<bool>,
    async_fsync_max_wait_ms: Option<u64>,
    wal_segment_size_bytes: Option<u64>,
    wal_preallocate_segments: Option<u32>,
    inline_history: Option<bool>,
    inline_history_max_bytes: Option<usize>,
    version_codec: Option<String>,
    version_codec_min_bytes: Option<usize>,
    version_codec_min_savings_bytes: Option<usize>,
    snapshot_pool_size: Option<usize>,
    snapshot_pool_max_age_ms: Option<u64>,
}

#[derive(Debug, Default)]
pub struct ProfileUpdate {
    pub database: Option<PathBuf>,
    pub page_size: Option<u32>,
    pub cache_pages: Option<usize>,
    pub synchronous: Option<SynchronousArg>,
    pub distinct_neighbors_default: Option<bool>,
    pub group_commit_max_writers: Option<usize>,
    pub group_commit_max_frames: Option<usize>,
    pub group_commit_max_wait_ms: Option<u64>,
    pub async_fsync: Option<bool>,
    pub async_fsync_max_wait_ms: Option<u64>,
    pub wal_segment_size_bytes: Option<u64>,
    pub wal_preallocate_segments: Option<u32>,
    pub inline_history: Option<bool>,
    pub inline_history_max_bytes: Option<usize>,
    pub version_codec: Option<VersionCodecArg>,
    pub version_codec_min_bytes: Option<usize>,
    pub version_codec_min_savings_bytes: Option<usize>,
    pub snapshot_pool_size: Option<usize>,
    pub snapshot_pool_max_age_ms: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read CLI config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse CLI config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: Box<toml::de::Error>,
    },
    #[error("failed to serialize CLI config: {source}")]
    Serialize {
        #[source]
        source: Box<toml::ser::Error>,
    },
    #[error("failed to write CLI config {path}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to create config directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("profile '{name}' not found")]
    ProfileNotFound { name: String },
    #[error("profile '{profile}' synchronous value '{value}' is invalid")]
    InvalidSynchronous { profile: String, value: String },
    #[error("profile '{profile}' version codec value '{value}' is invalid")]
    InvalidVersionCodec { profile: String, value: String },
    #[error("no config directory found; pass --config or set SOMBRA_CONFIG")]
    NoConfigPath,
}

pub fn default_config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|base| base.join("sombra").join("cli.toml"))
}
