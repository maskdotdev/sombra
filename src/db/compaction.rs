use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::error::Result;
use crate::db::metrics::PerformanceMetrics;
use crate::pager::Pager;
use crate::storage::heap::RecordStore;

pub enum CompactionMessage {
    Trigger,
    Shutdown,
}

pub struct CompactionConfig {
    pub enabled: bool,
    pub interval_secs: Option<u64>,
    pub threshold_percent: u8,
    pub batch_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: Some(300),
            threshold_percent: 50,
            batch_size: 100,
        }
    }
}

pub struct CompactionState {
    pub sender: Sender<CompactionMessage>,
    pub _compaction_thread: Option<thread::JoinHandle<()>>,
}

impl CompactionState {
    pub fn spawn(
        db_path: PathBuf,
        config: CompactionConfig,
        metrics: Arc<Mutex<PerformanceMetrics>>,
    ) -> Result<Arc<Mutex<Self>>> {
        if !config.enabled {
            let (sender, _receiver) = mpsc::channel();
            return Ok(Arc::new(Mutex::new(CompactionState {
                sender,
                _compaction_thread: None,
            })));
        }

        let (sender, receiver) = mpsc::channel();

        let compaction_thread = thread::spawn(move || {
            Self::compaction_loop(db_path, receiver, config, metrics);
        });

        Ok(Arc::new(Mutex::new(CompactionState {
            sender,
            _compaction_thread: Some(compaction_thread),
        })))
    }

    pub fn trigger_compaction(&self) -> Result<()> {
        self.sender
            .send(CompactionMessage::Trigger)
            .map_err(|_| crate::error::GraphError::Corruption("compaction channel closed".into()))
    }

    pub fn shutdown(&self) -> Result<()> {
        self.sender
            .send(CompactionMessage::Shutdown)
            .map_err(|_| crate::error::GraphError::Corruption("compaction channel closed".into()))
    }

    fn compaction_loop(
        db_path: PathBuf,
        receiver: Receiver<CompactionMessage>,
        config: CompactionConfig,
        metrics: Arc<Mutex<PerformanceMetrics>>,
    ) {
        let interval = Duration::from_secs(config.interval_secs.unwrap_or(300));

        loop {
            match receiver.recv_timeout(interval) {
                Ok(CompactionMessage::Trigger) => {
                    let _ = Self::perform_compaction(db_path.as_path(), &config, &metrics);
                }
                Ok(CompactionMessage::Shutdown) => {
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let _ = Self::perform_compaction(db_path.as_path(), &config, &metrics);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }

    fn perform_compaction(
        db_path: &Path,
        config: &CompactionConfig,
        metrics: &Arc<Mutex<PerformanceMetrics>>,
    ) -> Result<()> {
        let mut pager = Pager::open(db_path)?;
        let mut store = RecordStore::new(&mut pager);

        let candidates = store.identify_compaction_candidates(
            config.threshold_percent,
            config.batch_size,
        )?;

        if candidates.is_empty() {
            return Ok(());
        }

        let mut total_bytes_reclaimed = 0;
        let mut pages_compacted = 0;

        for page_id in candidates {
            match store.compact_page(page_id) {
                Ok(bytes_reclaimed) => {
                    total_bytes_reclaimed += bytes_reclaimed;
                    pages_compacted += 1;
                }
                Err(_) => {
                    continue;
                }
            }
        }

        pager.flush()?;

        if let Ok(mut m) = metrics.lock() {
            m.compactions_performed += 1;
            m.pages_compacted += pages_compacted;
            m.bytes_reclaimed += total_bytes_reclaimed as u64;
        }

        Ok(())
    }
}
