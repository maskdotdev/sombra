use siphasher::sip::SipHasher13;
use std::hash::Hasher;

/// Computes a deterministic SipHash64 over the provided row bytes.
pub fn row_hash64(bytes: &[u8]) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(0, 0);
    hasher.write(bytes);
    hasher.finish()
}
