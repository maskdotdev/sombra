## 📄 Document 2 — Stage 1: Bytes & Checksum

**Outcome:** production-ready encoding and checksum primitives (Stage 1), foundational **page constants**, and a clear validation plan so Stage 2 can plug them into the pager and page cache without surprises.

**Non-Goals**

- No pager, freelist, or IO yet (that is Part B / Stage 2).
- No attempt at SIMD, compression, or alternate checksum algorithms.
- No `unsafe` fast paths; keep the API ergonomic and panic-free in release builds.

---

### Part A — Stage 1: Bytes & Checksum

#### A0. Scope & Goals

- Order-preserving encodings for keys (`u64`, `i64`, `f64`, UTF-8 strings).
- Compact value encodings via unsigned varints and ZigZag-encoded signed integers.
- Stable `Cursor` helper to decode byte slices without repeated bounds checks.
- Checksum trait plus `crc32fast`-backed implementation and deterministic page CRC helper.
- Shared page constants & header layout used by Stage 2 pager work.

---

#### A1. Public APIs

**Crate `sombra-bytes`**

```rust
#![forbid(unsafe_code)]
pub mod ord {
    /// Big-endian encoding for lexicographic order preservation.
    pub fn put_u64_be(dst: &mut [u8], v: u64);
    pub fn get_u64_be(src: &[u8]) -> u64;

    /// Signed mapping: flip the sign bit, then BE encode.
    pub fn put_i64_be(dst: &mut [u8], v: i64);
    pub fn get_i64_be(src: &[u8]) -> i64;

    /// IEEE754 mapping: flip sign bit for negatives, then BE encode.
    pub fn put_f64_be(dst: &mut [u8], v: f64);
    pub fn get_f64_be(src: &[u8]) -> f64;

    /// Strings for keys: len (u32 BE) + UTF-8 bytes.
    pub fn put_str_key(dst: &mut Vec<u8>, s: &str);
    pub fn split_str_key(src: &[u8]) -> (&str, usize);
}

pub mod var {
    /// Unsigned LEB128-style varint (max 10 bytes for u64).
    pub fn encode_u64(v: u64, out: &mut Vec<u8>);
    pub fn decode_u64(src: &[u8], off: &mut usize) -> u64;

    /// ZigZag encoding for signed integers.
    pub fn encode_i64(v: i64, out: &mut Vec<u8>);
    pub fn decode_i64(src: &[u8], off: &mut usize) -> i64;
}

pub mod buf {
    /// Cursor over &[u8] for ergonomic decoding.
    pub struct Cursor<'a> {
        pub buf: &'a [u8],
        pub off: usize,
    }
    impl<'a> Cursor<'a> {
        pub fn new(buf: &'a [u8]) -> Self;
        pub fn take(&mut self, n: usize) -> &'a [u8];
        pub fn remaining(&self) -> usize;
    }
}
```

**Crate `sombra-checksum`**

```rust
#![forbid(unsafe_code)]
pub trait Checksum {
    fn reset(&mut self);
    fn update(&mut self, bytes: &[u8]);
    fn finalize(&self) -> u32;
}

pub struct Crc32Fast(crc32fast::Hasher);
impl Default for Crc32Fast { /* new + reset */ }
impl Checksum for Crc32Fast { /* delegate to inner hasher */ }

/// Page checksum: crc32(page_no||salt||payload_with_crc_zeroed)
pub fn page_crc32(page_no: u64, salt: u64, payload: &[u8]) -> u32;
```

The Stage 1 deliverable is these APIs fully implemented, documented, and covered by unit/property tests.

---

#### A2. Encoding Rules & Invariants

- **Order-preserving requirement:** lexicographic comparison on encoded bytes must match the natural ordering of the original value.
  - `u64`: encode with `to_be_bytes`; decode with `from_be_bytes`.
  - `i64`: flip the sign bit (`v ^ 0x8000_0000_0000_0000`) before BE-encoding; reverse the transformation on decode.
  - `f64`: reject NaNs in keys (debug assert) and map negatives by inverting the sign bit after transmuting to `u64`. Reuse IEEE754 bit pattern; use BE order.
  - **Zero and negative zero** must produce distinct encodings that preserve order (`-0.0` sorts before `+0.0`).
- **String keys:** prefix length as `u32::to_be_bytes`, append UTF-8 bytes verbatim. `split_str_key` must:
  - Read the length, ensure the slice contains that many bytes.
  - Validate UTF-8 (use `std::str::from_utf8`); panic on malformed data (Stage 2 code treats these as corruption).
  - Return the string slice plus the total bytes consumed (4 + len).
- **Key payloads never use varints.** Keys must remain fixed-order encodings so B-tree cursor seeks work via byte comparison alone.
- **Payload encodings** (values) are free to use varints/zigzag; they are not required to be order-preserving.
- Enforce **no `unsafe`** and avoid temporary heap allocations apart from `Vec::extend_from_slice` in `put_str_key`.

---

#### A3. Varints & ZigZag (module `var`)

- **Unsigned encoding:** classic LEB128; emit 7 bits per byte with MSB continuation flag. Highest byte must not be zero unless the value is zero.
- **Signed encoding:** ZigZag transform `((v << 1) ^ (v >> 63))` into `u64`, then reuse the unsigned encoder.
- **Decoding contract:**
  - Mutate `off` in place; all decode functions advance by the number of bytes consumed.
  - Detect truncated inputs: if `src` ends before a terminating byte, panic with a clear message (Stage 2 treats panics as corruption).
  - Reject encodings longer than 10 bytes for `u64` (overlong values). Overlong sequences should also panic.
  - Return to canonical ZigZag form when decoding signed values.
- Provide small helpers where useful (e.g., `fn read_byte` inside module scope) but keep API minimal.

---

#### A4. Buffer Cursor (`buf::Cursor`)

- `Cursor::new` stores slice reference and starts `off` at zero.
- `take(n)`:
  - Panics if `n` bytes are not available.
  - Returns subslice and advances `off` by `n`.
- `remaining()` returns `buf.len() - off`. Use safe arithmetic (`saturating_sub`) for resilience.
- Add convenience inherent methods for decoding (`read_var_u64`, etc.) only if Stage 2 needs them; otherwise keep scope tight.
- Derive `Debug` if it helps testing (optional).

---

#### A5. Checksum crate (`sombra-checksum`)

- `Checksum` trait is intentionally tiny; allow cloning the hasher by cloning the inner `crc32fast::Hasher`.
- `Crc32Fast::finalize` must leave the hasher reusable:
  - Option A: clone the inner hasher before finalizing.
  - Option B: call `inner.clone().finalize()` (as stub already does). Document behavior so callers know whether `finalize` is idempotent.
- `page_crc32(page_no, salt, payload)`:
  - Build a fixed 16-byte prefix: `page_no.to_be_bytes()` followed by `salt.to_be_bytes()`.
  - Expect `payload` to contain the entire page **header (with crc field zeroed)** concatenated with the body. Stage 2 will zero the CRC field before calling this helper.
  - Compute CRC over prefix + payload via `crc32fast::Hasher`.
  - Return the final value; caller writes it back into the header's CRC field.
- Unit tests should verify determinism: same inputs produce same CRC, changing any component changes the CRC.

---

#### A6. Page Constants & Header Layout

```rust
pub const DEFAULT_PAGE_SIZE: u32 = 8192; // 8 KiB
pub const PAGE_HDR_LEN: usize = 32;      // fixed header size

#[repr(u8)]
pub enum PageKind {
    Meta = 1,
    FreeList = 2,
    BTreeLeaf = 3,
    BTreeInternal = 4,
    Overflow = 5,
}
```

**Page header layout (bytes, offsets)** — the first `PAGE_HDR_LEN` bytes of every page:

| Offset | Size | Field           | Notes                                  |
| -----: | ---: | --------------- | -------------------------------------- |
|      0 |    4 | magic `b"SOMB"` | constant magic                         |
|      4 |    2 | format_version  | starts at `1`                          |
|      6 |    1 | page_kind       | matches `PageKind`                     |
|      7 |    1 | reserved        | zero for now                           |
|      8 |    4 | page_size       | redundant; validated on read           |
|     12 |    8 | page_no         | absolute page id                       |
|     20 |    8 | salt            | random per-db                          |
|     28 |    4 | crc32           | over header (crc field zeroed) + body  |

**Invariant:** the on-disk CRC is `page_crc32(page_no, salt, header_with_crc_zeroed + payload_bytes)`.

---

#### A7. Testing & QA Checklist

- **Unit tests (`sombra-bytes`)**
  - Round-trip tests for `put_*`/`get_*`, including boundary cases (`0`, `u64::MAX`, negatives, infinities, `-0.0`).
  - Sort tests: encode a shuffled list, sort encoded byte strings, decode, ensure numeric order aligns.
  - `put_str_key`/`split_str_key`: empty string, multibyte UTF-8, long strings; confirm byte consumption.
  - Varint/ZigZag golden cases: encode/decode best/worst-case values, assert canonical encoding length.
  - `Cursor::take` panics on over-read; `remaining` matches expectations.
- **Property tests (optional but recommended):**
  - Use `proptest` for 10k random values across `u64`, `i64`, `f64` (filter NaNs) checking round-trips and order.
  - Random payload fuzzing for varint decoder (stop short of 10 bytes) to ensure panic on truncation.
- **Checksum tests (`sombra-checksum`):**
  - Known vectors from `crc32fast` (reuse library’s test cases).
  - Recompute CRC after mutating header fields to ensure page CRC reacts accordingly.
- **Doc tests / README:**
  - Add module-level documentation strings summarizing behavior and usage from the snippets above.

---

#### A8. Integration Notes

- Export `sombra-bytes` and `sombra-checksum` from the workspace root `Cargo.toml` if not already present.
- Ensure `sombra-types` (Stage 0) depends on nothing from Stage 1; only Stage 2 crates (`sombra-pager`, `sombra-io`) should import these Stage 1 utilities.
- Update `docs/STAGE1_2_bytes_io_pager.md` once Stage 1 is implemented to mark highlights complete.
- After code lands, run `cargo fmt`, `cargo clippy`, and new unit tests in CI to avoid Stage 2 regressions.

---

Delivering Stage 1 gives us deterministic encoding/decoding utilities, a reusable checksum helper, and the static page header definition that Stage 2’s pager will rely on for correctness.
