#![forbid(unsafe_code)]

pub trait Checksum {
    fn reset(&mut self);
    fn update(&mut self, bytes: &[u8]);
    fn finalize(&self) -> u32;
}

pub struct Crc32Fast {
    inner: crc32fast::Hasher,
}

impl Default for Crc32Fast {
    fn default() -> Self {
        Self {
            inner: crc32fast::Hasher::new(),
        }
    }
}

impl Checksum for Crc32Fast {
    fn reset(&mut self) {
        self.inner.reset();
    }

    fn update(&mut self, bytes: &[u8]) {
        self.inner.update(bytes);
    }

    fn finalize(&self) -> u32 {
        self.inner.clone().finalize()
    }
}

pub fn page_crc32(page_no: u64, salt: u64, payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&page_no.to_be_bytes());
    hasher.update(&salt.to_be_bytes());
    hasher.update(payload);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_trait_roundtrip() {
        let mut c = Crc32Fast::default();
        c.update(b"hello");
        let first = c.finalize();
        c.update(b" world");
        let second = c.finalize();
        assert_ne!(first, second);
        c.reset();
        c.update(b"hello world");
        assert_eq!(c.finalize(), second);
    }

    #[test]
    fn page_crc32_changes_with_components() {
        let payload = vec![0u8; 16];
        let crc_a = page_crc32(1, 2, &payload);
        let crc_b = page_crc32(1, 2, &payload);
        assert_eq!(crc_a, crc_b);

        let mut different = payload.clone();
        different[0] = 1;
        assert_ne!(crc_a, page_crc32(1, 2, &different));
        assert_ne!(crc_a, page_crc32(3, 2, &payload));
        assert_ne!(crc_a, page_crc32(1, 3, &payload));
    }
}
