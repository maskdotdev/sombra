use sombra::{CompactionConfig, CompactionState, Config, GraphDB, PerformanceMetrics};
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;

#[test]
fn compaction_state_can_be_created() {
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_path_buf();
    
    let config = CompactionConfig {
        enabled: false,
        ..Default::default()
    };
    
    let metrics = Arc::new(Mutex::new(PerformanceMetrics::new()));
    let state = CompactionState::spawn(path, config, metrics).expect("spawn compaction state");
    
    drop(state);
}

#[test]
fn compaction_config_respects_enabled_flag() {
    let tmp = NamedTempFile::new().expect("create temp file");
    let path = tmp.path().to_path_buf();
    
    let config = CompactionConfig {
        enabled: false,
        interval_secs: Some(1),
        threshold_percent: 50,
        batch_size: 100,
    };
    
    let metrics = Arc::new(Mutex::new(PerformanceMetrics::new()));
    let _state = CompactionState::spawn(path, config, metrics).expect("spawn compaction state");
}

#[test]
fn config_has_compaction_settings() {
    let config = Config::production();
    assert!(config.enable_background_compaction);
    assert!(config.compaction_interval_secs.is_some());
    assert!(config.compaction_threshold_percent > 0);
    assert!(config.compaction_batch_size > 0);
}

#[test]
fn benchmark_config_disables_compaction() {
    let config = Config::benchmark();
    assert!(!config.enable_background_compaction);
}

#[test]
fn balanced_config_enables_compaction() {
    let config = Config::balanced();
    assert!(config.enable_background_compaction);
}

#[test]
fn metrics_include_compaction_counters() {
    let metrics = PerformanceMetrics::new();
    assert_eq!(metrics.compactions_performed, 0);
    assert_eq!(metrics.pages_compacted, 0);
    assert_eq!(metrics.bytes_reclaimed, 0);
}

#[test]
fn graphdb_can_open_with_compaction_config() {
    let tmp = NamedTempFile::new().expect("create temp file");
    let mut config = Config::production();
    config.enable_background_compaction = false;
    
    let _db = GraphDB::open_with_config(tmp.path(), config).expect("open database");
}
