# 🌿 Plain Leaf Record Encoding

The leaf-record refactor replaces prefix-delta encoding with a binary-search-friendly layout.
This document captures the exact on-page format, validation rules, and migration guardrails the
implementation must follow so later phases (`docs/build/leaf-record-plan.md`) have a stable
contract to execute against.

---

## Record layout

```
struct LeafRecord {
    key_len:  varint<u64>;
    val_len:  varint<u64>;
    key:      [u8; key_len];
    value:    [u8; val_len];
}
```

- Both `key_len` and `val_len` are encoded via `primitives::bytes::varint::encode_u64`.
- The decode path must reject:
  - varints that exceed 10 bytes (same guard as the shared varint helpers),
  - `key_len == 0` (keys are non-empty order-preserving encodings),
  - any record that would extend past the page’s `free_start`.
- Keys are stored verbatim; no prefix deltas or restart blocks are present.
- Values remain opaque byte blobs supplied by the `ValCodec`.
- Slot offsets still point at the first byte of `key_len`.

## Transition flag (retired)

- During Phases 1–2, leaf pages set `payload[FLAGS_OFFSET] & 0x01` to “plain”.
- The flag let readers distinguish new pages from legacy prefix-compressed ones.
- Phase 3 removed the legacy path, so new pages simply assume the plain layout and the flag bit
  is now reserved for potential future use (historical files without the bit set must be
  re-imported because the prefix decoder no longer exists).

## Validation & invariants

- Keys must remain strictly increasing when interpreted with the tree’s `KeyCodec`.
- `key_len + val_len + slot directory` must still fit between `free_start` and `free_end`.
- Since varints are variable width, encode helpers should bound-check the total record size
  before mutating the payload buffer.
- Corrupt varints or overflows raise `SombraError::Corruption`.

## Migration policy

- The storage layer is still pre-release, so we may refuse to open databases that contain
  legacy (prefix-compressed) leaves once Phase 1 lands.
- `storage::core::Db::bail_legacy_leaf_layout` emits the canonical error:

  > `invalid argument: plain leaf record layout required (prefix-compressed leaves detected; rebuild or re-import your data)`

- During Phases 1–2 we keep the legacy decode path behind the header flag to smooth the
  transition; Phase 3 removes it once all tests/benches confirm the new format.

---

See `docs/build/leaf-record-plan.md` for the phase-by-phase implementation checklist.
