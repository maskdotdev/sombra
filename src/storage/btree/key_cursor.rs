use crate::types::{Result, SombraError};

/// Lightweight cursor for walking varint-encoded slices without allocation.
#[derive(Clone, Copy, Debug)]
pub struct KeyCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> KeyCursor<'a> {
    /// Creates a new cursor over the provided buffer.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Reads an unsigned varint, returning an error with `truncated_msg` if input ends early.
    pub fn read_var_u64(&mut self, truncated_msg: &'static str) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        for _ in 0..10 {
            if self.pos >= self.buf.len() {
                return Err(SombraError::Corruption(truncated_msg));
            }
            let byte = self.buf[self.pos];
            self.pos += 1;
            result |= ((byte & 0x7f) as u64) << shift;
            if (byte & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(SombraError::Corruption("plain leaf varint too long"))
    }

    /// Takes `len` bytes from the cursor without copying.
    pub fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(SombraError::Corruption("plain leaf length overflow"))?;
        if end > self.buf.len() {
            return Err(SombraError::Corruption("plain leaf buffer truncated"));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::KeyCursor;
    use crate::types::{Result, SombraError};

    #[test]
    fn cursor_reads_varints_and_slices() -> Result<()> {
        let data = [0x81u8, 0x02, b'a', b'b', b'c'];
        let mut cursor = KeyCursor::new(&data);
        let len = cursor.read_var_u64("truncated")?;
        assert_eq!(len, 0x101);
        let slice = cursor.take(3)?;
        assert_eq!(slice, b"abc");
        Ok(())
    }

    #[test]
    fn cursor_rejects_truncated_varint() {
        let data = [0x81];
        let mut cursor = KeyCursor::new(&data);
        let err = cursor
            .read_var_u64("too short")
            .expect_err("truncated varint");
        assert!(matches!(err, SombraError::Corruption(_)));
    }

    #[test]
    fn cursor_rejects_slice_overflow() {
        let data = [0x00];
        let mut cursor = KeyCursor::new(&data);
        cursor.take(1).expect("first byte ok");
        let err = cursor.take(1).expect_err("overflow");
        assert!(matches!(err, SombraError::Corruption(_)));
    }
}
