use std::collections::HashMap;

#[derive(Default)]
pub(super) struct LeafAllocatorCache {
    entries: HashMap<PageId, LeafAllocatorSnapshot>,
}

impl LeafAllocatorCache {
    pub fn take(&mut self, page_id: PageId) -> Option<LeafAllocatorSnapshot> {
        self.entries.remove(&page_id)
    }

    pub fn insert(&mut self, page_id: PageId, snapshot: LeafAllocatorSnapshot) {
        self.entries.insert(page_id, snapshot);
    }
}
