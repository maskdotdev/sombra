use crate::primitives::io::FileIo;
use crate::types::{
    page::{self, PageHeader, PageKind, PAGE_HDR_LEN},
    page_crc32, Lsn, PageId, Result, SombraError,
};
use rand::{rngs::OsRng, RngCore};
use std::convert::TryInto;
use std::fmt;
use std::io::ErrorKind;
use std::ops::Range;

const META_SALT: Range<usize> = PAGE_HDR_LEN..PAGE_HDR_LEN + 8;
const META_PAGE_SIZE: Range<usize> = PAGE_HDR_LEN + 8..PAGE_HDR_LEN + 12;
const META_FORMAT_VERSION: Range<usize> = PAGE_HDR_LEN + 12..PAGE_HDR_LEN + 14;
const META_RESERVED: Range<usize> = PAGE_HDR_LEN + 14..PAGE_HDR_LEN + 16;
const META_FREE_HEAD: Range<usize> = PAGE_HDR_LEN + 16..PAGE_HDR_LEN + 24;
const META_NEXT_PAGE: Range<usize> = PAGE_HDR_LEN + 24..PAGE_HDR_LEN + 32;
const META_LAST_CHECKPOINT_LSN: Range<usize> = PAGE_HDR_LEN + 32..PAGE_HDR_LEN + 40;
const META_WAL_SALT: Range<usize> = PAGE_HDR_LEN + 40..PAGE_HDR_LEN + 48;
const META_WAL_POLICY_FLAGS: Range<usize> = PAGE_HDR_LEN + 48..PAGE_HDR_LEN + 52;
const META_RESERVED_2: Range<usize> = PAGE_HDR_LEN + 52..PAGE_HDR_LEN + 56;
const META_DICT_STR_TO_ID_ROOT: Range<usize> = PAGE_HDR_LEN + 56..PAGE_HDR_LEN + 64;
const META_DICT_ID_TO_STR_ROOT: Range<usize> = PAGE_HDR_LEN + 64..PAGE_HDR_LEN + 72;
const META_DICT_NEXT_STR_ID: Range<usize> = PAGE_HDR_LEN + 72..PAGE_HDR_LEN + 76;
const META_STORAGE_FLAGS: Range<usize> = PAGE_HDR_LEN + 76..PAGE_HDR_LEN + 80;
const META_STORAGE_NODES_ROOT: Range<usize> = PAGE_HDR_LEN + 80..PAGE_HDR_LEN + 88;
const META_STORAGE_EDGES_ROOT: Range<usize> = PAGE_HDR_LEN + 88..PAGE_HDR_LEN + 96;
const META_STORAGE_ADJ_FWD_ROOT: Range<usize> = PAGE_HDR_LEN + 96..PAGE_HDR_LEN + 104;
const META_STORAGE_ADJ_REV_ROOT: Range<usize> = PAGE_HDR_LEN + 104..PAGE_HDR_LEN + 112;
const META_STORAGE_DEGREE_ROOT: Range<usize> = PAGE_HDR_LEN + 112..PAGE_HDR_LEN + 120;
const META_INDEX_CATALOG_ROOT: Range<usize> = PAGE_HDR_LEN + 120..PAGE_HDR_LEN + 128;
const META_LABEL_INDEX_ROOT: Range<usize> = PAGE_HDR_LEN + 128..PAGE_HDR_LEN + 136;
const META_PROP_CHUNK_ROOT: Range<usize> = PAGE_HDR_LEN + 136..PAGE_HDR_LEN + 144;
const META_PROP_BTREE_ROOT: Range<usize> = PAGE_HDR_LEN + 144..PAGE_HDR_LEN + 152;
const META_STORAGE_NEXT_NODE_ID: Range<usize> = PAGE_HDR_LEN + 152..PAGE_HDR_LEN + 160;
const META_STORAGE_NEXT_EDGE_ID: Range<usize> = PAGE_HDR_LEN + 160..PAGE_HDR_LEN + 168;
const META_STORAGE_INLINE_PROP_BLOB: Range<usize> = PAGE_HDR_LEN + 168..PAGE_HDR_LEN + 172;
const META_STORAGE_INLINE_PROP_VALUE: Range<usize> = PAGE_HDR_LEN + 172..PAGE_HDR_LEN + 176;
const META_STORAGE_DDL_EPOCH: Range<usize> = PAGE_HDR_LEN + 176..PAGE_HDR_LEN + 184;
const META_VERSION_LOG_ROOT: Range<usize> = PAGE_HDR_LEN + 184..PAGE_HDR_LEN + 192;
const META_STORAGE_NEXT_VERSION_PTR: Range<usize> = PAGE_HDR_LEN + 192..PAGE_HDR_LEN + 200;
const META_RESERVED_3: Range<usize> = PAGE_HDR_LEN + 200..PAGE_HDR_LEN + 208;

/// Database metadata stored in page 0 containing configuration and root pointers.
///
/// This structure holds all essential database metadata including page size, version info,
/// freelist head, dictionary roots, storage roots, and various configuration flags.
#[derive(Clone, Debug, PartialEq)]
pub struct Meta {
    /// Size of each page in bytes.
    pub page_size: u32,
    /// Random salt value used for page checksums.
    pub salt: u64,
    /// Database format version number.
    pub format_version: u16,
    /// Page ID of the head of the freelist chain.
    pub free_head: PageId,
    /// Next page ID to be allocated.
    pub next_page: PageId,
    /// LSN of the last successful checkpoint.
    pub last_checkpoint_lsn: Lsn,
    /// Random salt value for WAL integrity checks.
    pub wal_salt: u64,
    /// Configuration flags for WAL policy.
    pub wal_policy_flags: u32,
    /// Root page ID for the string-to-ID dictionary B-tree.
    pub dict_str_to_id_root: PageId,
    /// Root page ID for the ID-to-string dictionary B-tree.
    pub dict_id_to_str_root: PageId,
    /// Next string ID to be allocated in the dictionary.
    pub dict_next_str_id: u32,
    /// Configuration flags for storage layer.
    pub storage_flags: u32,
    /// Root page ID for the nodes B-tree.
    pub storage_nodes_root: PageId,
    /// Root page ID for the edges B-tree.
    pub storage_edges_root: PageId,
    /// Root page ID for the forward adjacency B-tree.
    pub storage_adj_fwd_root: PageId,
    /// Root page ID for the reverse adjacency B-tree.
    pub storage_adj_rev_root: PageId,
    /// Root page ID for the degree tracking B-tree.
    pub storage_degree_root: PageId,
    /// Root page ID for the index catalog B-tree.
    pub storage_index_catalog_root: PageId,
    /// Root page ID for the label index B-tree.
    pub storage_label_index_root: PageId,
    /// Root page ID for the property chunk B-tree.
    pub storage_prop_chunk_root: PageId,
    /// Root page ID for the property B-tree.
    pub storage_prop_btree_root: PageId,
    /// Root page ID for the version log heap.
    pub storage_version_log_root: PageId,
    /// Next node ID to be allocated.
    pub storage_next_node_id: u64,
    /// Next edge ID to be allocated.
    pub storage_next_edge_id: u64,
    /// Next version pointer to be allocated.
    pub storage_next_version_ptr: u64,
    /// Size threshold for inline property blobs.
    pub storage_inline_prop_blob: u32,
    /// Size threshold for inline property values.
    pub storage_inline_prop_value: u32,
    /// Catalog DDL epoch used to invalidate cached index metadata.
    pub storage_ddl_epoch: u64,
}

/// Creates a new database metadata page with default values and writes it to page 0.
///
/// Generates random salts, initializes all root pointers to null, and sets default configuration.
/// The metadata page is immediately written to disk and synced.
pub fn create_meta(io: &dyn FileIo, page_size: u32) -> Result<Meta> {
    if (page_size as usize) < PAGE_HDR_LEN {
        return Err(SombraError::Invalid("page size smaller than header"));
    }
    let mut rng = OsRng;
    let salt = rng.next_u64();
    let wal_salt = rng.next_u64();
    let meta = Meta {
        page_size,
        salt,
        format_version: page::PAGE_FORMAT_VERSION,
        free_head: PageId(0),
        next_page: PageId(1),
        last_checkpoint_lsn: Lsn(0),
        wal_salt,
        wal_policy_flags: 0,
        dict_str_to_id_root: PageId(0),
        dict_id_to_str_root: PageId(0),
        dict_next_str_id: 1,
        storage_flags: 0,
        storage_nodes_root: PageId(0),
        storage_edges_root: PageId(0),
        storage_adj_fwd_root: PageId(0),
        storage_adj_rev_root: PageId(0),
        storage_degree_root: PageId(0),
        storage_index_catalog_root: PageId(0),
        storage_label_index_root: PageId(0),
        storage_prop_chunk_root: PageId(0),
        storage_prop_btree_root: PageId(0),
        storage_version_log_root: PageId(0),
        storage_next_node_id: 1,
        storage_next_edge_id: 1,
        storage_next_version_ptr: 1,
        storage_inline_prop_blob: 128,
        storage_inline_prop_value: 48,
        storage_ddl_epoch: 0,
    };
    let mut buf = vec![0u8; page_size as usize];
    write_meta_page(&mut buf, &meta)?;
    io.write_at(0, &buf)?;
    io.sync_all()?;
    Ok(meta)
}

/// Loads and verifies the database metadata from page 0.
///
/// Reads page 0, verifies the CRC checksum, and parses the metadata structure.
/// Returns an error if the page is truncated, corrupted, or has invalid checksums.
pub fn load_meta(io: &dyn FileIo, page_size: u32) -> Result<Meta> {
    if (page_size as usize) < PAGE_HDR_LEN {
        return Err(SombraError::Invalid("page size smaller than header"));
    }
    let mut buf = vec![0u8; page_size as usize];
    match io.read_at(0, &mut buf) {
        Ok(()) => {}
        Err(SombraError::Io(err)) if err.kind() == ErrorKind::UnexpectedEof => {
            return Err(SombraError::Corruption("meta page truncated"));
        }
        Err(SombraError::Io(err)) => return Err(SombraError::Io(err)),
        Err(err) => return Err(err),
    }
    read_meta_page(&buf)
}

/// Encodes metadata into a page buffer with proper header and CRC checksum.
///
/// Serializes all metadata fields into the provided buffer and computes a CRC32 checksum
/// for integrity verification. The buffer must be at least `page_size` bytes.
pub fn write_meta_page(buf: &mut [u8], meta: &Meta) -> Result<()> {
    if buf.len() < PAGE_HDR_LEN {
        return Err(SombraError::Invalid("meta buffer too small"));
    }
    let page_size = meta.page_size as usize;
    if buf.len() < page_size {
        return Err(SombraError::Invalid("meta buffer too small"));
    }
    buf[..page_size].fill(0);
    let header =
        PageHeader::new(PageId(0), PageKind::Meta, meta.page_size, meta.salt)?.with_crc32(0);
    header.encode(&mut buf[..PAGE_HDR_LEN])?;
    buf[META_SALT].copy_from_slice(&meta.salt.to_be_bytes());
    buf[META_PAGE_SIZE].copy_from_slice(&meta.page_size.to_be_bytes());
    buf[META_FORMAT_VERSION].copy_from_slice(&meta.format_version.to_be_bytes());
    buf[META_RESERVED].fill(0);
    buf[META_FREE_HEAD].copy_from_slice(&meta.free_head.0.to_be_bytes());
    buf[META_NEXT_PAGE].copy_from_slice(&meta.next_page.0.to_be_bytes());
    buf[META_LAST_CHECKPOINT_LSN].copy_from_slice(&meta.last_checkpoint_lsn.0.to_be_bytes());
    buf[META_WAL_SALT].copy_from_slice(&meta.wal_salt.to_be_bytes());
    buf[META_WAL_POLICY_FLAGS].copy_from_slice(&meta.wal_policy_flags.to_be_bytes());
    buf[META_RESERVED_2].fill(0);
    buf[META_DICT_STR_TO_ID_ROOT].copy_from_slice(&meta.dict_str_to_id_root.0.to_be_bytes());
    buf[META_DICT_ID_TO_STR_ROOT].copy_from_slice(&meta.dict_id_to_str_root.0.to_be_bytes());
    buf[META_DICT_NEXT_STR_ID].copy_from_slice(&meta.dict_next_str_id.to_be_bytes());
    buf[META_STORAGE_FLAGS].copy_from_slice(&meta.storage_flags.to_be_bytes());
    buf[META_STORAGE_NODES_ROOT].copy_from_slice(&meta.storage_nodes_root.0.to_be_bytes());
    buf[META_STORAGE_EDGES_ROOT].copy_from_slice(&meta.storage_edges_root.0.to_be_bytes());
    buf[META_STORAGE_ADJ_FWD_ROOT].copy_from_slice(&meta.storage_adj_fwd_root.0.to_be_bytes());
    buf[META_STORAGE_ADJ_REV_ROOT].copy_from_slice(&meta.storage_adj_rev_root.0.to_be_bytes());
    buf[META_STORAGE_DEGREE_ROOT].copy_from_slice(&meta.storage_degree_root.0.to_be_bytes());
    buf[META_INDEX_CATALOG_ROOT].copy_from_slice(&meta.storage_index_catalog_root.0.to_be_bytes());
    buf[META_LABEL_INDEX_ROOT].copy_from_slice(&meta.storage_label_index_root.0.to_be_bytes());
    buf[META_PROP_CHUNK_ROOT].copy_from_slice(&meta.storage_prop_chunk_root.0.to_be_bytes());
    buf[META_PROP_BTREE_ROOT].copy_from_slice(&meta.storage_prop_btree_root.0.to_be_bytes());
    buf[META_STORAGE_NEXT_NODE_ID].copy_from_slice(&meta.storage_next_node_id.to_be_bytes());
    buf[META_STORAGE_NEXT_EDGE_ID].copy_from_slice(&meta.storage_next_edge_id.to_be_bytes());
    buf[META_VERSION_LOG_ROOT].copy_from_slice(&meta.storage_version_log_root.0.to_be_bytes());
    buf[META_STORAGE_NEXT_VERSION_PTR]
        .copy_from_slice(&meta.storage_next_version_ptr.to_be_bytes());
    buf[META_STORAGE_INLINE_PROP_BLOB]
        .copy_from_slice(&meta.storage_inline_prop_blob.to_be_bytes());
    buf[META_STORAGE_INLINE_PROP_VALUE]
        .copy_from_slice(&meta.storage_inline_prop_value.to_be_bytes());
    buf[META_STORAGE_DDL_EPOCH].copy_from_slice(&meta.storage_ddl_epoch.to_be_bytes());
    buf[META_RESERVED_3].fill(0);
    page::clear_crc32(&mut buf[..PAGE_HDR_LEN])?;
    let crc = page_crc32(PageId(0).0, meta.salt, &buf[..page_size]);
    buf[page::header::CRC32].copy_from_slice(&crc.to_be_bytes());
    Ok(())
}

/// Decodes metadata from a page buffer and verifies its integrity.
///
/// Parses the page header, validates the CRC checksum, and deserializes all metadata fields.
/// Returns an error if the page kind is wrong, the checksum fails, or reserved fields are non-zero.
pub fn read_meta_page(buf: &[u8]) -> Result<Meta> {
    if buf.len() < PAGE_HDR_LEN {
        return Err(SombraError::Corruption("meta page truncated"));
    }
    let header = PageHeader::decode(&buf[..PAGE_HDR_LEN])?;
    if header.kind != PageKind::Meta {
        return Err(SombraError::Corruption("meta page has wrong kind"));
    }
    let len = header.page_size as usize;
    if buf.len() < len {
        return Err(SombraError::Corruption("meta page truncated"));
    }
    let mut scratch = buf[..len].to_vec();
    page::clear_crc32(&mut scratch[..PAGE_HDR_LEN])?;
    let crc = page_crc32(header.page_no.0, header.salt, &scratch);
    if crc != header.crc32 {
        return Err(SombraError::Corruption("meta page crc mismatch"));
    }
    let salt = u64::from_be_bytes(buf[META_SALT].try_into().unwrap());
    let page_size = u32::from_be_bytes(buf[META_PAGE_SIZE].try_into().unwrap());
    let format_version = u16::from_be_bytes(buf[META_FORMAT_VERSION].try_into().unwrap());
    let reserved = u16::from_be_bytes(buf[META_RESERVED].try_into().unwrap());
    if reserved != 0 {
        return Err(SombraError::Corruption("meta reserved field non-zero"));
    }
    let free_head = PageId(u64::from_be_bytes(buf[META_FREE_HEAD].try_into().unwrap()));
    let next_page = PageId(u64::from_be_bytes(buf[META_NEXT_PAGE].try_into().unwrap()));
    let last_checkpoint_lsn = Lsn(u64::from_be_bytes(
        buf[META_LAST_CHECKPOINT_LSN].try_into().unwrap(),
    ));
    let wal_salt = u64::from_be_bytes(buf[META_WAL_SALT].try_into().unwrap());
    let wal_policy_flags = u32::from_be_bytes(buf[META_WAL_POLICY_FLAGS].try_into().unwrap());
    let dict_str_to_id_root = PageId(u64::from_be_bytes(
        buf[META_DICT_STR_TO_ID_ROOT].try_into().unwrap(),
    ));
    let dict_id_to_str_root = PageId(u64::from_be_bytes(
        buf[META_DICT_ID_TO_STR_ROOT].try_into().unwrap(),
    ));
    let reserved2 = u32::from_be_bytes(buf[META_RESERVED_2].try_into().unwrap());
    if reserved2 != 0 {
        return Err(SombraError::Corruption("meta reserved2 field non-zero"));
    }
    let dict_next_str_id = u32::from_be_bytes(buf[META_DICT_NEXT_STR_ID].try_into().unwrap());
    let storage_flags = u32::from_be_bytes(buf[META_STORAGE_FLAGS].try_into().unwrap());
    let storage_nodes_root = PageId(u64::from_be_bytes(
        buf[META_STORAGE_NODES_ROOT].try_into().unwrap(),
    ));
    let storage_edges_root = PageId(u64::from_be_bytes(
        buf[META_STORAGE_EDGES_ROOT].try_into().unwrap(),
    ));
    let storage_adj_fwd_root = PageId(u64::from_be_bytes(
        buf[META_STORAGE_ADJ_FWD_ROOT].try_into().unwrap(),
    ));
    let storage_adj_rev_root = PageId(u64::from_be_bytes(
        buf[META_STORAGE_ADJ_REV_ROOT].try_into().unwrap(),
    ));
    let storage_degree_root = PageId(u64::from_be_bytes(
        buf[META_STORAGE_DEGREE_ROOT].try_into().unwrap(),
    ));
    let storage_index_catalog_root = PageId(u64::from_be_bytes(
        buf[META_INDEX_CATALOG_ROOT].try_into().unwrap(),
    ));
    let storage_label_index_root = PageId(u64::from_be_bytes(
        buf[META_LABEL_INDEX_ROOT].try_into().unwrap(),
    ));
    let storage_prop_chunk_root = PageId(u64::from_be_bytes(
        buf[META_PROP_CHUNK_ROOT].try_into().unwrap(),
    ));
    let storage_prop_btree_root = PageId(u64::from_be_bytes(
        buf[META_PROP_BTREE_ROOT].try_into().unwrap(),
    ));
    let storage_version_log_root = PageId(u64::from_be_bytes(
        buf[META_VERSION_LOG_ROOT].try_into().unwrap(),
    ));
    let storage_next_node_id =
        u64::from_be_bytes(buf[META_STORAGE_NEXT_NODE_ID].try_into().unwrap());
    let storage_next_edge_id =
        u64::from_be_bytes(buf[META_STORAGE_NEXT_EDGE_ID].try_into().unwrap());
    let storage_next_version_ptr =
        u64::from_be_bytes(buf[META_STORAGE_NEXT_VERSION_PTR].try_into().unwrap());
    let storage_inline_prop_blob =
        u32::from_be_bytes(buf[META_STORAGE_INLINE_PROP_BLOB].try_into().unwrap());
    let storage_inline_prop_value =
        u32::from_be_bytes(buf[META_STORAGE_INLINE_PROP_VALUE].try_into().unwrap());
    let storage_ddl_epoch = u64::from_be_bytes(buf[META_STORAGE_DDL_EPOCH].try_into().unwrap());
    if buf[META_RESERVED_3].iter().any(|b| *b != 0) {
        return Err(SombraError::Corruption("meta reserved3 field non-zero"));
    }
    Ok(Meta {
        page_size,
        salt,
        format_version,
        free_head,
        next_page,
        last_checkpoint_lsn,
        wal_salt,
        wal_policy_flags,
        dict_str_to_id_root,
        dict_id_to_str_root,
        dict_next_str_id: dict_next_str_id.max(1),
        storage_flags,
        storage_nodes_root,
        storage_edges_root,
        storage_adj_fwd_root,
        storage_adj_rev_root,
        storage_degree_root,
        storage_index_catalog_root,
        storage_label_index_root,
        storage_prop_chunk_root,
        storage_prop_btree_root,
        storage_version_log_root,
        storage_next_node_id: storage_next_node_id.max(1),
        storage_next_edge_id: storage_next_edge_id.max(1),
        storage_next_version_ptr: storage_next_version_ptr.max(1),
        storage_inline_prop_blob: storage_inline_prop_blob.max(32),
        storage_inline_prop_value: storage_inline_prop_value.max(8),
        storage_ddl_epoch,
    })
}

impl fmt::Display for Meta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Meta(page_size={}, salt={}, format_version={}, free_head={}, next_page={}, last_checkpoint_lsn={}, wal_salt={}, wal_policy_flags={}, dict_str_to_id_root={}, dict_id_to_str_root={}, dict_next_str_id={}, storage_flags={}, storage_nodes_root={}, storage_edges_root={}, storage_adj_fwd_root={}, storage_adj_rev_root={}, storage_degree_root={}, storage_index_catalog_root={}, storage_label_index_root={}, storage_prop_chunk_root={}, storage_prop_btree_root={}, storage_version_log_root={}, storage_next_node_id={}, storage_next_edge_id={}, storage_next_version_ptr={}, storage_inline_prop_blob={}, storage_inline_prop_value={}, storage_ddl_epoch={})",
            self.page_size,
            self.salt,
            self.format_version,
            self.free_head.0,
            self.next_page.0,
            self.last_checkpoint_lsn.0,
            self.wal_salt,
            self.wal_policy_flags,
            self.dict_str_to_id_root.0,
            self.dict_id_to_str_root.0,
            self.dict_next_str_id,
            self.storage_flags,
            self.storage_nodes_root.0,
            self.storage_edges_root.0,
            self.storage_adj_fwd_root.0,
            self.storage_adj_rev_root.0,
            self.storage_degree_root.0,
            self.storage_index_catalog_root.0,
            self.storage_label_index_root.0,
            self.storage_prop_chunk_root.0,
            self.storage_prop_btree_root.0,
            self.storage_version_log_root.0,
            self.storage_next_node_id,
            self.storage_next_edge_id,
            self.storage_next_version_ptr,
            self.storage_inline_prop_blob,
            self.storage_inline_prop_value,
            self.storage_ddl_epoch,
        )
    }
}
