use parking_lot::RwLock;
use std::sync::Arc;

use crate::types::PageId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameState {
    Hot,
    Cold,
    Test,
}

pub struct Frame {
    pub id: Option<PageId>,
    pub buf: Arc<RwLock<Box<[u8]>>>,
    pub state: FrameState,
    pub reference: bool,
    pub dirty: bool,
    pub pin_count: u32,
    pub pending_checkpoint: bool,
    pub newly_allocated: bool,
    pub needs_refresh: bool,
}

impl Frame {
    pub fn new(page_size: usize) -> Self {
        Self {
            id: None,
            buf: Arc::new(RwLock::new(vec![0u8; page_size].into_boxed_slice())),
            state: FrameState::Test,
            reference: false,
            dirty: false,
            pin_count: 0,
            pending_checkpoint: false,
            newly_allocated: false,
            needs_refresh: false,
        }
    }
}
