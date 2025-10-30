use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::error::{acquire_lock, Result};

pub type TxId = u64;

pub(crate) enum ControlMessage {
    Commit(CommitRequest),
    #[allow(dead_code)]
    Shutdown,
}

pub(crate) struct CommitRequest {
    #[allow(dead_code)]
    pub tx_id: TxId,
    pub notifier: Arc<(Mutex<bool>, Condvar)>,
}

impl std::fmt::Debug for GroupCommitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCommitState").finish()
    }
}

pub(crate) struct GroupCommitState {
    pub sender: Sender<ControlMessage>,
    pub _committer_thread: Option<thread::JoinHandle<()>>,
}

impl GroupCommitState {
    pub fn spawn(db_path: PathBuf, timeout_ms: u64) -> Result<Arc<Mutex<Self>>> {
        let (sender, receiver): (Sender<ControlMessage>, Receiver<ControlMessage>) =
            mpsc::channel();

        let committer_thread = thread::spawn(move || {
            Self::group_commit_loop(db_path, receiver, timeout_ms);
        });

        Ok(Arc::new(Mutex::new(GroupCommitState {
            sender,
            _committer_thread: Some(committer_thread),
        })))
    }

    #[allow(dead_code)]
    pub fn shutdown(&self) -> Result<()> {
        self.sender
            .send(ControlMessage::Shutdown)
            .map_err(|_| crate::error::GraphError::Corruption("control channel closed".into()))
    }

    fn group_commit_loop(db_path: PathBuf, receiver: Receiver<ControlMessage>, timeout_ms: u64) {
        let wal_path = db_path.with_extension("wal");

        // Adaptive timeout: start with short timeout for low latency
        let short_timeout = Duration::from_micros(100); // 100µs for single-threaded
        let long_timeout = Duration::from_millis(timeout_ms); // Full timeout for batching
        let mut current_timeout = short_timeout;

        loop {
            let mut pending_commits = Vec::new();

            match receiver.recv_timeout(current_timeout) {
                Ok(ControlMessage::Commit(first_commit)) => {
                    pending_commits.push(first_commit);

                    // Immediately check for more commits (batching opportunity)
                    while let Ok(msg) = receiver.try_recv() {
                        match msg {
                            ControlMessage::Commit(commit_req) => {
                                pending_commits.push(commit_req);
                            }
                            ControlMessage::Shutdown => {
                                Self::flush_pending_commits(&wal_path, pending_commits);
                                return;
                            }
                        }
                    }

                    // Adaptive timeout: if we batched >1 commit, increase timeout
                    // to catch more batching opportunities
                    if pending_commits.len() > 1 {
                        current_timeout = long_timeout;
                    } else {
                        // Single commit: reduce timeout for lower latency
                        current_timeout = short_timeout;
                    }
                }
                Ok(ControlMessage::Shutdown) => {
                    Self::flush_pending_commits(&wal_path, Vec::new());
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }

            Self::flush_pending_commits(&wal_path, pending_commits);
        }
    }

    fn flush_pending_commits(wal_path: &PathBuf, pending_commits: Vec<CommitRequest>) {
        if pending_commits.is_empty() {
            return;
        }

        if let Ok(file) = std::fs::OpenOptions::new().write(true).open(wal_path) {
            let _ = file.sync_data();
        }

        for commit_req in pending_commits {
            let (lock, cvar) = &*commit_req.notifier;
            if let Ok(mut done) = acquire_lock(lock) {
                *done = true;
                cvar.notify_one();
            }
        }
    }
}
