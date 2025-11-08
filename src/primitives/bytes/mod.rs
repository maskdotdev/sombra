#![forbid(unsafe_code)]
//! Encoding, varint, and buffer utilities shared across storage layers.

pub mod ord {
    //! Order-preserving encoders for numeric and string keys.

    use core::convert::TryInto;

    const U64_LEN: usize = core::mem::size_of::<u64>();
    const SIGN_BIT: u64 = 1 << 63;

    /// Big-endian encoding for lexicographic order preservation.
    pub fn put_u64_be(dst: &mut [u8], v: u64) {
        assert!(dst.len() >= U64_LEN, "destination too small");
        dst[..U64_LEN].copy_from_slice(&v.to_be_bytes());
    }

    /// Decodes a u64 from big-endian byte order.
    pub fn get_u64_be(src: &[u8]) -> u64 {
        let head = src
            .get(..U64_LEN)
            .unwrap_or_else(|| panic!("u64 source shorter than 8 bytes (have {})", src.len()));
        let bytes: [u8; U64_LEN] = head.try_into().unwrap();
        u64::from_be_bytes(bytes)
    }

    /// Encodes a signed i64 with order preservation (flip sign bit for sorting).
    pub fn put_i64_be(dst: &mut [u8], v: i64) {
        let flipped = (v as u64) ^ SIGN_BIT;
        put_u64_be(dst, flipped);
    }

    /// Decodes a signed i64 with order preservation.
    pub fn get_i64_be(src: &[u8]) -> i64 {
        let flipped = get_u64_be(src);
        let raw = flipped ^ SIGN_BIT;
        raw as i64
    }

    /// Encodes an f64 with order preservation (NaN not allowed).
    pub fn put_f64_be(dst: &mut [u8], v: f64) {
        debug_assert!(!v.is_nan(), "NaN keys are not allowed");
        let bits = encode_f64_bits(v);
        put_u64_be(dst, bits);
    }

    /// Decodes an f64 with order preservation.
    pub fn get_f64_be(src: &[u8]) -> f64 {
        let bits = get_u64_be(src);
        let decoded = decode_f64_bits(bits);
        f64::from_bits(decoded)
    }

    /// Appends a length-prefixed string key to a byte vector.
    pub fn put_str_key(dst: &mut Vec<u8>, s: &str) {
        let len = s.len();
        assert!(
            len <= u32::MAX as usize,
            "string key too long (>{} bytes)",
            u32::MAX
        );
        dst.extend_from_slice(&(len as u32).to_be_bytes());
        dst.extend_from_slice(s.as_bytes());
    }

    /// Splits a length-prefixed string key, returning the string and its total length in bytes.
    pub fn split_str_key(src: &[u8]) -> (&str, usize) {
        const LEN_LEN: usize = core::mem::size_of::<u32>();
        assert!(
            src.len() >= LEN_LEN,
            "string key slice shorter than length prefix"
        );
        let len = u32::from_be_bytes(
            src[..LEN_LEN]
                .try_into()
                .expect("prefix conversion should not fail"),
        ) as usize;
        let end = LEN_LEN + len;
        assert!(
            src.len() >= end,
            "string key slice truncated (need {} bytes, have {})",
            len,
            src.len().saturating_sub(LEN_LEN)
        );
        let body = &src[LEN_LEN..end];
        let s = core::str::from_utf8(body).expect("string key not valid UTF-8");
        (s, end)
    }

    fn encode_f64_bits(v: f64) -> u64 {
        let bits = v.to_bits();
        if bits & SIGN_BIT != 0 {
            !bits
        } else {
            bits ^ SIGN_BIT
        }
    }

    fn decode_f64_bits(encoded: u64) -> u64 {
        if encoded & SIGN_BIT != 0 {
            encoded ^ SIGN_BIT
        } else {
            !encoded
        }
    }
}

pub mod var {
    //! Unsigned varints and ZigZag signed integers.

    #[allow(clippy::inline_always)]
    #[inline]
    fn push_byte(byte: u8, out: &mut Vec<u8>) {
        out.push(byte);
    }

    /// Encodes a u64 as an unsigned varint.
    pub fn encode_u64(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                push_byte(byte, out);
                break;
            } else {
                push_byte(byte | 0x80, out);
            }
        }
    }

    /// Decodes a u64 varint from a slice, updating the offset.
    pub fn decode_u64(src: &[u8], off: &mut usize) -> u64 {
        let mut result = 0u64;
        let mut shift = 0u32;
        for i in 0..10 {
            let idx = *off;
            if idx >= src.len() {
                panic!("varint decode truncated at byte {}", i);
            }
            let byte = src[idx];
            *off += 1;
            let payload = (byte & 0x7f) as u64;
            result |= payload << shift;
            if (byte & 0x80) == 0 {
                if i == 9 && payload > 1 {
                    panic!("varint overflow (more than 64 bits)");
                }
                return result;
            }
            shift += 7;
            if shift >= 64 {
                panic!("varint too long (exceeds 64 bits)");
            }
        }
        panic!("varint too long (exceeded 10 bytes)");
    }

    /// Encodes an i64 as a ZigZag-encoded varint.
    pub fn encode_i64(v: i64, out: &mut Vec<u8>) {
        let zigzag = ((v << 1) ^ (v >> 63)) as u64;
        encode_u64(zigzag, out);
    }

    /// Decodes a ZigZag-encoded i64 varint from a slice, updating the offset.
    pub fn decode_i64(src: &[u8], off: &mut usize) -> i64 {
        let zigzag = decode_u64(src, off);
        ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64))
    }
}

pub mod buf {
    //! A simple slice-backed cursor for ergonomic parsing.

    use core::fmt;

    /// A cursor for reading bytes from a slice with offset tracking.
    pub struct Cursor<'a> {
        /// The underlying byte slice.
        pub buf: &'a [u8],
        /// Current read offset.
        pub off: usize,
    }

    impl<'a> Cursor<'a> {
        /// Creates a new cursor starting at offset 0.
        pub fn new(buf: &'a [u8]) -> Self {
            Self { buf, off: 0 }
        }

        /// Takes the next `n` bytes from the cursor, advancing the offset.
        pub fn take(&mut self, n: usize) -> &'a [u8] {
            let end = self
                .off
                .checked_add(n)
                .expect("cursor offset overflow during take");
            if end > self.buf.len() {
                panic!(
                    "cursor take beyond buffer: need {}, remaining {}",
                    n,
                    self.remaining()
                );
            }
            let slice = &self.buf[self.off..end];
            self.off = end;
            slice
        }

        /// Returns the number of bytes remaining in the buffer.
        pub fn remaining(&self) -> usize {
            self.buf.len().saturating_sub(self.off)
        }
    }

    impl<'a> fmt::Debug for Cursor<'a> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Cursor")
                .field("off", &self.off)
                .field("remaining", &self.remaining())
                .finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{buf::Cursor, ord, var};
    use proptest::prelude::*;

    #[test]
    fn u64_roundtrip() {
        let mut dst = [0u8; 8];
        ord::put_u64_be(&mut dst, 123456789);
        assert_eq!(ord::get_u64_be(&dst), 123456789);
    }

    #[test]
    fn i64_roundtrip() {
        let mut dst = [0u8; 8];
        let values = [i64::MIN, -1, 0, 1, i64::MAX];
        for &v in &values {
            ord::put_i64_be(&mut dst, v);
            assert_eq!(ord::get_i64_be(&dst), v);
        }
    }

    #[test]
    fn f64_ordering_handles_neg_zero() {
        let mut neg = [0u8; 8];
        let mut pos = [0u8; 8];
        ord::put_f64_be(&mut neg, -0.0);
        ord::put_f64_be(&mut pos, 0.0);
        assert!(neg < pos, "negative zero must sort before positive zero");
        assert_eq!(ord::get_f64_be(&neg), -0.0);
        assert_eq!(ord::get_f64_be(&pos), 0.0);
    }

    #[test]
    fn str_key_roundtrip() {
        let s = "héllo";
        let mut buf = Vec::new();
        ord::put_str_key(&mut buf, s);
        let (decoded, consumed) = ord::split_str_key(&buf);
        assert_eq!(decoded, s);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn varint_roundtrip_edges() {
        let mut buf = Vec::new();
        var::encode_u64(0, &mut buf);
        let mut off = 0;
        assert_eq!(var::decode_u64(&buf, &mut off), 0);
        assert_eq!(off, buf.len());

        buf.clear();
        var::encode_u64(u64::MAX, &mut buf);
        off = 0;
        assert_eq!(var::decode_u64(&buf, &mut off), u64::MAX);
        assert_eq!(off, buf.len());

        buf.clear();
        var::encode_i64(i64::MIN, &mut buf);
        off = 0;
        assert_eq!(var::decode_i64(&buf, &mut off), i64::MIN);
    }

    #[test]
    #[should_panic(expected = "cursor take beyond buffer")]
    fn cursor_take_panics_on_overread() {
        let mut cur = Cursor::new(&[1, 2, 3]);
        let _ = cur.take(4);
    }

    #[test]
    #[should_panic(expected = "varint decode truncated")]
    fn varint_decode_rejects_truncated() {
        let data = vec![0x80]; // continuation bit without payload
        let mut off = 0;
        let _ = var::decode_u64(&data, &mut off);
    }

    #[test]
    #[should_panic(expected = "varint too long")]
    fn varint_decode_rejects_too_long() {
        let data = vec![0x81; 11];
        let mut off = 0;
        let _ = var::decode_u64(&data, &mut off);
    }

    proptest! {
        #[test]
        fn order_preserving_u64_prop(xs in proptest::collection::vec(any::<u64>(), 1..64)) {
            let mut encoded: Vec<([u8; 8], u64)> = xs
                .iter()
                .map(|&v| {
                    let mut buf = [0u8; 8];
                    ord::put_u64_be(&mut buf, v);
                    (buf, v)
                })
                .collect();
            encoded.sort_by(|a, b| a.0.cmp(&b.0));
            let decoded: Vec<u64> = encoded
                .iter()
                .map(|(buf, _)| ord::get_u64_be(buf))
                .collect();
            let mut expected = xs.clone();
            expected.sort();
            prop_assert_eq!(decoded, expected);
        }

        #[test]
        fn order_preserving_i64_prop(xs in proptest::collection::vec(any::<i64>(), 1..64)) {
            let mut encoded: Vec<([u8; 8], i64)> = xs
                .iter()
                .map(|&v| {
                    let mut buf = [0u8; 8];
                    ord::put_i64_be(&mut buf, v);
                    (buf, v)
                })
                .collect();
            encoded.sort_by(|a, b| a.0.cmp(&b.0));
            let decoded: Vec<i64> = encoded
                .iter()
                .map(|(buf, _)| ord::get_i64_be(buf))
                .collect();
            let mut expected = xs.clone();
            expected.sort();
            prop_assert_eq!(decoded, expected);
        }

        #[test]
        fn order_preserving_f64_prop(xs in proptest::collection::vec(
            any::<f64>().prop_filter("finite", |v| v.is_finite() && !v.is_nan()),
            1..64
        )) {
            let mut encoded: Vec<([u8; 8], f64)> = xs
                .iter()
                .map(|&v| {
                    let mut buf = [0u8; 8];
                    ord::put_f64_be(&mut buf, v);
                    (buf, v)
                })
                .collect();
            encoded.sort_by(|a, b| a.0.cmp(&b.0));
            let decoded: Vec<f64> = encoded
                .iter()
                .map(|(buf, _)| ord::get_f64_be(buf))
                .collect();
            let mut expected = xs.clone();
            expected.sort_by(|a, b| a.partial_cmp(b).unwrap());
            prop_assert_eq!(decoded, expected);
        }

        #[test]
        fn str_key_roundtrip_prop(s in proptest::collection::vec(any::<char>(), 0..64).prop_map(|chars| chars.into_iter().collect::<String>())) {
            let mut buf = Vec::new();
            ord::put_str_key(&mut buf, &s);
            let (decoded, consumed) = ord::split_str_key(&buf);
            prop_assert_eq!(decoded, s);
            prop_assert_eq!(consumed, buf.len());
        }

        #[test]
        fn varint_roundtrip_u64_prop(v in any::<u64>()) {
            let mut buf = Vec::new();
            var::encode_u64(v, &mut buf);
            let mut off = 0;
            let decoded = var::decode_u64(&buf, &mut off);
            prop_assert_eq!(decoded, v);
            prop_assert_eq!(off, buf.len());
        }

        #[test]
        fn varint_roundtrip_i64_prop(v in any::<i64>()) {
            let mut buf = Vec::new();
            var::encode_i64(v, &mut buf);
            let mut off = 0;
            let decoded = var::decode_i64(&buf, &mut off);
            prop_assert_eq!(decoded, v);
            prop_assert_eq!(off, buf.len());
        }
    }
}
