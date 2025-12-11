//! Pool of reusable byte buffers for reducing allocation churn during writes.

/// Pool of reusable byte buffers.
#[derive(Default)]
pub struct BufferPool {
    buffers: Vec<Vec<u8>>,
    max_buffers: usize,
}

impl BufferPool {
    /// Creates a new buffer pool with the given maximum capacity.
    pub fn new(max_buffers: usize) -> Self {
        Self {
            buffers: Vec::with_capacity(max_buffers),
            max_buffers,
        }
    }

    /// Acquires a buffer, either from the pool or freshly allocated.
    pub fn acquire(&mut self) -> Vec<u8> {
        self.buffers.pop().unwrap_or_default()
    }

    /// Acquires a buffer with at least the given capacity.
    pub fn acquire_with_capacity(&mut self, capacity: usize) -> Vec<u8> {
        let mut buf = self.acquire();
        buf.clear();
        if buf.capacity() < capacity {
            buf.reserve(capacity - buf.capacity());
        }
        buf
    }

    /// Returns a buffer to the pool for reuse.
    pub fn release(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.buffers.len() < self.max_buffers {
            self.buffers.push(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_returns_empty_buffer_when_pool_empty() {
        let mut pool = BufferPool::new(4);
        let buf = pool.acquire();
        assert!(buf.is_empty());
    }

    #[test]
    fn acquire_with_capacity_reserves_space() {
        let mut pool = BufferPool::new(4);
        let buf = pool.acquire_with_capacity(100);
        assert!(buf.capacity() >= 100);
        assert!(buf.is_empty());
    }

    #[test]
    fn release_and_acquire_reuses_buffer() {
        let mut pool = BufferPool::new(4);
        let mut buf = pool.acquire_with_capacity(256);
        buf.extend_from_slice(b"hello");
        let cap = buf.capacity();
        pool.release(buf);

        let reused = pool.acquire();
        assert!(reused.is_empty());
        assert_eq!(reused.capacity(), cap);
    }

    #[test]
    fn pool_respects_max_buffers() {
        let mut pool = BufferPool::new(2);
        let b1 = pool.acquire_with_capacity(64);
        let b2 = pool.acquire_with_capacity(64);
        let b3 = pool.acquire_with_capacity(64);

        pool.release(b1);
        pool.release(b2);
        pool.release(b3); // This one should be dropped

        assert_eq!(pool.buffers.len(), 2);
    }
}
