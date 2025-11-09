/// Trait implemented by key types that can be encoded for storage in the B+ tree.
pub trait KeyCodec: Sized {
    /// Encode `key` into `out` using the order-preserving representation.
    fn encode_key(key: &Self, out: &mut Vec<u8>);

    /// Compare two encoded keys.
    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering;

    /// Decode a key from its encoded representation.
    fn decode_key(bytes: &[u8]) -> Result<Self>;
}

/// Trait implemented by value types that can be stored in the B+ tree.
pub trait ValCodec: Sized {
    /// Encode `value` into `out`.
    fn encode_val(value: &Self, out: &mut Vec<u8>);

    /// Decode a value from `src`.
    fn decode_val(src: &[u8]) -> Result<Self>;
}

/// Configuration knobs for the B+ tree.
#[derive(Clone, Debug)]
pub struct BTreeOptions {
    /// Target fill percentage for pages (0-100)
    pub page_fill_target: u8,
    /// Minimum fill percentage for internal pages before merging (0-100)
    pub internal_min_fill: u8,
    /// Whether to verify checksums when reading pages
    pub checksum_verify_on_read: bool,
    /// Optional root page ID for an existing tree
    pub root_page: Option<PageId>,
    /// Whether to attempt in-place leaf edits (insert/delete) before rebuilding
    pub in_place_leaf_edits: bool,
}

impl Default for BTreeOptions {
    fn default() -> Self {
        Self {
            page_fill_target: 85,
            internal_min_fill: 40,
            checksum_verify_on_read: true,
            root_page: None,
            in_place_leaf_edits: false,
        }
    }
}

/// Minimal internal state for a B+ tree instance.
pub struct BTree<K: KeyCodec, V: ValCodec> {
    pub(super) store: Arc<dyn PageStore>,
    pub(super) root: AtomicU64,
    pub(super) page_size: usize,
    pub(super) salt: u64,
    pub(super) options: BTreeOptions,
    pub(super) stats: Arc<BTreeStats>,
    pub(super) _marker: PhantomData<(K, V)>,
}

/// Item to insert via [`BTree::put_many`].
pub struct PutItem<'a, K: KeyCodec, V: ValCodec> {
    /// Key reference to insert.
    pub key: &'a K,
    /// Value reference to insert.
    pub value: &'a V,
}

#[derive(Clone)]
pub(super) struct PathEntry {
    pub(super) page_id: PageId,
    pub(super) slot_index: usize,
}

pub(super) struct LeafCache {
    pub(super) leaf_id: PageId,
    pub(super) path: Vec<PathEntry>,
}

pub(super) struct SlotView<'a> {
    payload: &'a [u8],
    slots: page::SlotDirectory<'a>,
    extents: page::SlotExtents,
}

impl<'a> SlotView<'a> {
    pub(super) fn new(header: &page::Header, data: &'a [u8]) -> Result<Self> {
        let payload = page::payload(data)?;
        let slots = header.slot_directory(data)?;
        let extents = page::SlotExtents::build(header, payload, &slots)?;
        Ok(Self {
            payload,
            slots,
            extents,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(super) fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub(super) fn slots(&self) -> &page::SlotDirectory<'a> {
        &self.slots
    }

    pub(super) fn slice(&self, slot_idx: usize) -> Result<&'a [u8]> {
        self.extents.record_slice(self.payload, slot_idx)
    }
}

enum LeafInsert {
    Done {
        new_first_key: Option<Vec<u8>>,
    },
    Split {
        left_min: Vec<u8>,
        right_min: Vec<u8>,
        right_page: PageId,
    },
}

enum InPlaceInsertResult {
    Applied {
        new_first_key: Option<Vec<u8>>,
    },
    NotApplied,
}

enum BorrowResult {
    Borrowed,
    InsufficientDonor,
    LayoutOverflow,
}

enum InternalInsert {
    Done,
    Split {
        left_min: Vec<u8>,
        right_min: Vec<u8>,
        right_page: PageId,
    },
}

struct InternalLayout {
    records: Vec<u8>,
    offsets: Vec<u16>,
    lengths: Vec<u16>,
    free_start: u16,
    free_end: u16,
}

struct InPlaceDeleteResult {
    free_start: u16,
    free_end: u16,
}

struct LeafSnapshot {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    low_fence: Vec<u8>,
    high_fence: Vec<u8>,
}

struct InternalSnapshot {
    entries: Vec<(Vec<u8>, PageId)>,
    low_fence: Vec<u8>,
    high_fence: Vec<u8>,
}
