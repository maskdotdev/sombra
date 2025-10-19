use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::error::Result;

pub type TxId = u64;

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
    pub sender: Sender<CommitRequest>,
    pub _committer_thread: Option<thread::JoinHandle<()>>,
}

impl GroupCommitState {
    pub fn spawn(db_path: PathBuf, timeout_ms: u64) -> Result<Arc<Mutex<Self>>> {
        let (sender, receiver): (Sender<CommitRequest>, Receiver<CommitRequest>) = mpsc::channel();

        let committer_thread = thread::spawn(move || {
            Self::group_commit_loop(db_path, receiver, timeout_ms);
        });

        Ok(Arc::new(Mutex::new(GroupCommitState {
            sender,
            _committer_thread: Some(committer_thread),
        })))
    }

    fn group_commit_loop(db_path: PathBuf, receiver: Receiver<CommitRequest>, timeout_ms: u64) {
        let wal_path = db_path.with_extension("wal");

        loop {
            let mut pending_commits = Vec::new();

            match receiver.recv_timeout(Duration::from_millis(timeout_ms)) {
                Ok(first_commit) => {
                    pending_commits.push(first_commit);

                    while let Ok(commit_req) = receiver.try_recv() {
                        pending_commits.push(commit_req);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }

            if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&wal_path) {
                let _ = file.sync_data();
            }

            for commit_req in pending_commits {
                let (lock, cvar) = &*commit_req.notifier;
                let mut done = lock.lock().unwrap();
                *done = true;
                cvar.notify_one();
            }
        }
    }
}
