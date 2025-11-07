use crate::tree::{KeyCodec, ValCodec};
use sombra_types::{Result, SombraError};

impl KeyCodec for u64 {
    fn encode_key(key: &Self, out: &mut Vec<u8>) {
        out.extend_from_slice(&key.to_be_bytes());
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    fn decode_key(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            return Err(SombraError::Corruption("u64 key length mismatch"));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(arr))
    }
}

impl ValCodec for u64 {
    fn encode_val(value: &Self, out: &mut Vec<u8>) {
        out.extend_from_slice(&value.to_be_bytes());
    }

    fn decode_val(src: &[u8]) -> Result<Self> {
        if src.len() != 8 {
            return Err(SombraError::Corruption("u64 value length mismatch"));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(src);
        Ok(u64::from_be_bytes(arr))
    }
}

impl KeyCodec for Vec<u8> {
    fn encode_key(key: &Self, out: &mut Vec<u8>) {
        out.extend_from_slice(key);
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    fn decode_key(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

impl ValCodec for Vec<u8> {
    fn encode_val(value: &Self, out: &mut Vec<u8>) {
        out.extend_from_slice(value);
    }

    fn decode_val(src: &[u8]) -> Result<Self> {
        Ok(src.to_vec())
    }
}
