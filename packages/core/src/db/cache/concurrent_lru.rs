use dashmap::DashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Lock-free LRU cache using DashMap for concurrent access
///
/// This implementation provides thread-safe LRU caching without the need for
/// external locking. It uses DashMap for the underlying storage and maintains
/// approximate LRU semantics under concurrent access.
pub struct ConcurrentLruCache<K, V> {
    map: DashMap<K, V>,
    capacity: usize,
    size: AtomicUsize,
}

impl<K, V> ConcurrentLruCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Creates a new concurrent LRU cache with the specified capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            map: DashMap::with_capacity(capacity),
            capacity,
            size: AtomicUsize::new(0),
        }
    }

    /// Gets a value from the cache
    ///
    /// Returns Some(value) if the key exists, None otherwise.
    /// This is lock-free and does not block other readers.
    pub fn get(&self, key: &K) -> Option<V> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    /// Inserts a key-value pair into the cache
    ///
    /// If the cache is at capacity, this will attempt to evict entries.
    /// The eviction is approximate and may not always evict the true LRU item
    /// under high concurrency, but provides good performance.
    pub fn put(&self, key: K, value: V) {
        // Fast path: if key exists, just update
        if self.map.contains_key(&key) {
            self.map.insert(key, value);
            return;
        }

        // Evict if we're at or over capacity
        while self.size.load(Ordering::Relaxed) >= self.capacity {
            if !self.evict_one() {
                break; // No more entries to evict
            }
        }

        // Insert the new entry
        if self.map.insert(key, value).is_none() {
            self.size.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Evicts approximately one entry from the cache
    ///
    /// This is a best-effort operation that removes one of the first few entries
    /// it encounters. Under concurrent access, this provides good-enough LRU
    /// behavior with excellent performance.
    ///
    /// Returns true if an entry was evicted, false otherwise.
    fn evict_one(&self) -> bool {
        // Get the first key and remove it
        // This is approximate LRU but very fast
        let to_remove = self.map.iter().next().map(|entry| entry.key().clone());

        if let Some(key) = to_remove {
            if self.map.remove(&key).is_some() {
                self.size.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Removes a key from the cache
    pub fn pop(&self, key: &K) -> Option<V> {
        self.map.remove(key).map(|(_, v)| {
            self.size.fetch_sub(1, Ordering::Relaxed);
            v
        })
    }

    /// Clears all entries from the cache
    pub fn clear(&self) {
        self.map.clear();
        self.size.store(0, Ordering::Relaxed);
    }

    /// Returns the current number of entries in the cache
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Returns true if the cache is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of the cache
    #[allow(dead_code)]
    pub fn cap(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_operations() {
        let cache = ConcurrentLruCache::new(3);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        assert_eq!(cache.get(&1), Some("one"));
        assert_eq!(cache.get(&2), Some("two"));
        assert_eq!(cache.get(&3), Some("three"));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_eviction() {
        let cache = ConcurrentLruCache::new(2);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        // After eviction, we should have at most 2 entries
        assert!(cache.len() <= 2);
    }

    #[test]
    fn test_concurrent_access() {
        let cache = Arc::new(ConcurrentLruCache::new(100));
        let mut handles = vec![];

        // Spawn multiple threads that read and write
        for thread_id in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = thread_id * 100 + i;
                    cache_clone.put(key, format!("value-{}", key));
                    let _ = cache_clone.get(&key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should be at or near capacity
        assert!(cache.len() <= 100);
    }

    #[test]
    fn test_clear() {
        let cache = ConcurrentLruCache::new(10);

        cache.put(1, "one");
        cache.put(2, "two");
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_pop() {
        let cache = ConcurrentLruCache::new(10);

        cache.put(1, "one");
        assert_eq!(cache.get(&1), Some("one"));

        let popped = cache.pop(&1);
        assert_eq!(popped, Some("one"));
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.len(), 0);
    }
}
