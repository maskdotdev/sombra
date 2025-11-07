#![forbid(unsafe_code)]

use std::fmt;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeId(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EdgeId(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct StrId(pub u32);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct LabelId(pub u32);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TypeId(pub u32);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PropId(pub u32);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PageId(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Lsn(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct VRef {
    pub start_page: PageId,
    pub n_pages: u32,
    pub len: u32,
    pub checksum: u32,
}

#[derive(thiserror::Error, Debug)]
pub enum SombraError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("corruption: {0}")]
    Corruption(&'static str),
    #[error("invalid argument: {0}")]
    Invalid(&'static str),
    #[error("not found")]
    NotFound,
}

pub type Result<T> = std::result::Result<T, SombraError>;

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for StrId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for LabelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for TypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for PropId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for LabelId {
    fn from(value: u32) -> Self {
        LabelId(value)
    }
}

impl From<LabelId> for u32 {
    fn from(value: LabelId) -> Self {
        value.0
    }
}

impl From<u32> for TypeId {
    fn from(value: u32) -> Self {
        TypeId(value)
    }
}

impl From<TypeId> for u32 {
    fn from(value: TypeId) -> Self {
        value.0
    }
}

impl From<u32> for PropId {
    fn from(value: u32) -> Self {
        PropId(value)
    }
}

impl From<PropId> for u32 {
    fn from(value: PropId) -> Self {
        value.0
    }
}

pub mod page {
    //! Shared on-disk page metadata used by pager components.

    use core::convert::{TryFrom, TryInto};

    use super::{PageId, Result, SombraError};

    pub const PAGE_MAGIC: [u8; 4] = *b"SOMB";
    pub const PAGE_FORMAT_VERSION: u16 = 1;
    pub const DEFAULT_PAGE_SIZE: u32 = 8192;
    pub const PAGE_HDR_LEN: usize = 32;

    pub mod header {
        //! Byte offsets for fixed header fields.
        use core::ops::Range;

        pub const MAGIC: Range<usize> = 0..4;
        pub const FORMAT_VERSION: Range<usize> = 4..6;
        pub const PAGE_KIND: usize = 6;
        pub const RESERVED: usize = 7;
        pub const PAGE_SIZE: Range<usize> = 8..12;
        pub const PAGE_NO: Range<usize> = 12..20;
        pub const SALT: Range<usize> = 20..28;
        pub const CRC32: Range<usize> = 28..32;
    }

    #[repr(u8)]
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum PageKind {
        Meta = 1,
        FreeList = 2,
        BTreeLeaf = 3,
        BTreeInternal = 4,
        Overflow = 5,
    }

    impl PageKind {
        pub const fn as_u8(self) -> u8 {
            self as u8
        }
    }

    impl TryFrom<u8> for PageKind {
        type Error = SombraError;

        fn try_from(value: u8) -> Result<Self> {
            match value {
                1 => Ok(PageKind::Meta),
                2 => Ok(PageKind::FreeList),
                3 => Ok(PageKind::BTreeLeaf),
                4 => Ok(PageKind::BTreeInternal),
                5 => Ok(PageKind::Overflow),
                _ => Err(SombraError::Corruption("unknown page kind")),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PageHeader {
        pub format_version: u16,
        pub kind: PageKind,
        pub page_size: u32,
        pub page_no: PageId,
        pub salt: u64,
        pub crc32: u32,
    }

    impl PageHeader {
        pub fn new(page_no: PageId, kind: PageKind, page_size: u32, salt: u64) -> Result<Self> {
            if (page_size as usize) < PAGE_HDR_LEN {
                return Err(SombraError::Invalid("page size smaller than header"));
            }
            Ok(Self {
                format_version: PAGE_FORMAT_VERSION,
                kind,
                page_size,
                page_no,
                salt,
                crc32: 0,
            })
        }

        pub fn with_crc32(mut self, crc32: u32) -> Self {
            self.crc32 = crc32;
            self
        }

        pub fn encode(&self, dst: &mut [u8]) -> Result<()> {
            if dst.len() < PAGE_HDR_LEN {
                return Err(SombraError::Invalid("page header buffer too small"));
            }
            let hdr = &mut dst[..PAGE_HDR_LEN];
            hdr[header::MAGIC].copy_from_slice(&PAGE_MAGIC);
            hdr[header::FORMAT_VERSION].copy_from_slice(&self.format_version.to_be_bytes());
            hdr[header::PAGE_KIND] = self.kind.as_u8();
            hdr[header::RESERVED] = 0;
            hdr[header::PAGE_SIZE].copy_from_slice(&self.page_size.to_be_bytes());
            hdr[header::PAGE_NO].copy_from_slice(&self.page_no.0.to_be_bytes());
            hdr[header::SALT].copy_from_slice(&self.salt.to_be_bytes());
            hdr[header::CRC32].copy_from_slice(&self.crc32.to_be_bytes());
            Ok(())
        }

        pub fn decode(src: &[u8]) -> Result<Self> {
            if src.len() < PAGE_HDR_LEN {
                return Err(SombraError::Corruption("page header truncated"));
            }
            let hdr = &src[..PAGE_HDR_LEN];
            let magic: [u8; 4] = hdr[header::MAGIC].try_into().unwrap();
            if magic != PAGE_MAGIC {
                return Err(SombraError::Corruption("invalid page magic"));
            }
            let format_version =
                u16::from_be_bytes(hdr[header::FORMAT_VERSION].try_into().unwrap());
            if format_version != PAGE_FORMAT_VERSION {
                return Err(SombraError::Corruption("unsupported page format version"));
            }
            if hdr[header::RESERVED] != 0 {
                return Err(SombraError::Corruption(
                    "page header reserved byte not zero",
                ));
            }
            let kind = PageKind::try_from(hdr[header::PAGE_KIND])?;
            let page_size = u32::from_be_bytes(hdr[header::PAGE_SIZE].try_into().unwrap());
            if (page_size as usize) < PAGE_HDR_LEN {
                return Err(SombraError::Corruption("page size smaller than header"));
            }
            let page_no = PageId(u64::from_be_bytes(hdr[header::PAGE_NO].try_into().unwrap()));
            let salt = u64::from_be_bytes(hdr[header::SALT].try_into().unwrap());
            let crc32 = u32::from_be_bytes(hdr[header::CRC32].try_into().unwrap());
            Ok(Self {
                format_version,
                kind,
                page_size,
                page_no,
                salt,
                crc32,
            })
        }
    }

    pub fn clear_crc32(buf: &mut [u8]) -> Result<()> {
        if buf.len() < header::CRC32.end {
            return Err(SombraError::Invalid("page header buffer too small"));
        }
        buf[header::CRC32].fill(0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{page, page::PageHeader, page::PageKind, PageId};
    use std::convert::TryFrom;

    #[test]
    fn page_header_roundtrip() {
        let mut buf = [0u8; page::PAGE_HDR_LEN];
        let header = PageHeader::new(
            PageId(42),
            PageKind::BTreeLeaf,
            page::DEFAULT_PAGE_SIZE,
            777,
        )
        .unwrap()
        .with_crc32(0xDEADBEEF);
        header.encode(&mut buf).unwrap();
        let decoded = PageHeader::decode(&buf).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn clear_crc32_zeroes_field() {
        let mut buf = [0xFFu8; page::PAGE_HDR_LEN];
        page::clear_crc32(&mut buf).unwrap();
        assert!(buf[page::header::CRC32].iter().all(|&b| b == 0));
    }

    #[test]
    fn page_kind_from_u8_rejects_unknown() {
        assert!(PageKind::try_from(0).is_err());
    }
}
