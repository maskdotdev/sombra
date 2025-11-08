use crate::primitives::pager::{ReadGuard, WriteGuard};
use crate::storage::vstore::VStore;
use crate::types::{PropId, Result, SombraError, VRef};

use super::types::{PropEntry, PropValue, PropValueOwned};

pub const TYPE_NULL: u8 = 0;
pub const TYPE_BOOL: u8 = 1;
pub const TYPE_INT: u8 = 2;
pub const TYPE_FLOAT: u8 = 3;
pub const TYPE_STR: u8 = 4;
pub const TYPE_BYTES: u8 = 5;
pub const TYPE_DATETIME: u8 = 6;
pub const TYPE_DATE: u8 = 7;

pub struct PropEncodeResult {
    pub bytes: Vec<u8>,
    pub spill_vrefs: Vec<VRef>,
}

#[derive(Clone, Debug)]
pub struct RawProp {
    pub prop: PropId,
    pub value: RawPropValue,
}

#[derive(Clone, Debug)]
pub enum RawPropValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    StrInline(Vec<u8>),
    StrVRef(VRef),
    BytesInline(Vec<u8>),
    BytesVRef(VRef),
    Date(i64),
    DateTime(i64),
}

pub fn encode_props<'a>(
    entries: &[PropEntry<'a>],
    inline_value_limit: usize,
    vstore: &VStore,
    tx: &mut WriteGuard<'_>,
) -> Result<PropEncodeResult> {
    if entries.is_empty() {
        return Ok(PropEncodeResult {
            bytes: vec![0],
            spill_vrefs: Vec::new(),
        });
    }
    let mut ordered: Vec<PropEntry<'_>> = entries.iter().cloned().collect();
    ordered.sort_by(|a, b| a.prop.0.cmp(&b.prop.0));
    for pair in ordered.windows(2) {
        if pair[0].prop == pair[1].prop {
            return Err(SombraError::Invalid("duplicate property id"));
        }
    }
    let mut bytes = Vec::with_capacity(ordered.len() * 8);
    write_var_u64(ordered.len() as u64, &mut bytes);
    let mut spill_vrefs = Vec::new();
    for entry in ordered {
        write_var_u64(entry.prop.0 as u64, &mut bytes);
        match entry.value {
            PropValue::Null => bytes.push(TYPE_NULL),
            PropValue::Bool(v) => {
                bytes.push(TYPE_BOOL);
                bytes.push(if v { 1 } else { 0 });
            }
            PropValue::Int(v) => {
                bytes.push(TYPE_INT);
                write_var_i64(v, &mut bytes);
            }
            PropValue::Float(v) => {
                bytes.push(TYPE_FLOAT);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            PropValue::Str(s) => {
                encode_bytes_like(
                    &mut bytes,
                    TYPE_STR,
                    s.as_bytes(),
                    inline_value_limit,
                    vstore,
                    tx,
                    &mut spill_vrefs,
                )?;
            }
            PropValue::Bytes(b) => {
                encode_bytes_like(
                    &mut bytes,
                    TYPE_BYTES,
                    b,
                    inline_value_limit,
                    vstore,
                    tx,
                    &mut spill_vrefs,
                )?;
            }
            PropValue::Date(v) => {
                bytes.push(TYPE_DATE);
                write_var_i64(v, &mut bytes);
            }
            PropValue::DateTime(v) => {
                bytes.push(TYPE_DATETIME);
                write_var_i64(v, &mut bytes);
            }
        }
    }
    Ok(PropEncodeResult { bytes, spill_vrefs })
}

pub fn encode_props_owned(
    entries: &[(PropId, PropValueOwned)],
    inline_value_limit: usize,
    vstore: &VStore,
    tx: &mut WriteGuard<'_>,
) -> Result<PropEncodeResult> {
    if entries.is_empty() {
        return Ok(PropEncodeResult {
            bytes: vec![0],
            spill_vrefs: Vec::new(),
        });
    }
    let mut ordered: Vec<&(PropId, PropValueOwned)> = entries.iter().collect();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    for pair in ordered.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(SombraError::Invalid("duplicate property id"));
        }
    }
    let mut temp_entries = Vec::with_capacity(ordered.len());
    for (prop, value) in ordered {
        let prop_value = match value {
            PropValueOwned::Null => PropValue::Null,
            PropValueOwned::Bool(v) => PropValue::Bool(*v),
            PropValueOwned::Int(v) => PropValue::Int(*v),
            PropValueOwned::Float(v) => PropValue::Float(*v),
            PropValueOwned::Str(v) => PropValue::Str(v.as_str()),
            PropValueOwned::Bytes(v) => PropValue::Bytes(v.as_slice()),
            PropValueOwned::Date(v) => PropValue::Date(*v),
            PropValueOwned::DateTime(v) => PropValue::DateTime(*v),
        };
        temp_entries.push(PropEntry::new(*prop, prop_value));
    }
    encode_props(&temp_entries, inline_value_limit, vstore, tx)
}

pub fn decode_raw(buf: &[u8]) -> Result<Vec<RawProp>> {
    let mut cursor = 0usize;
    let entry_count = read_var_u64(buf, &mut cursor)?;
    let mut props = Vec::with_capacity(entry_count as usize);
    for _ in 0..entry_count {
        let prop_raw = read_var_u64(buf, &mut cursor)?;
        if prop_raw > u32::MAX as u64 {
            return Err(SombraError::Corruption("property id overflow"));
        }
        let prop = PropId(prop_raw as u32);
        if cursor >= buf.len() {
            return Err(SombraError::Corruption("property record truncated"));
        }
        let type_tag = buf[cursor];
        cursor += 1;
        let value = match type_tag {
            TYPE_NULL => RawPropValue::Null,
            TYPE_BOOL => {
                if cursor >= buf.len() {
                    return Err(SombraError::Corruption("bool payload truncated"));
                }
                let byte = buf[cursor];
                cursor += 1;
                if byte > 1 {
                    return Err(SombraError::Corruption("bool payload invalid"));
                }
                RawPropValue::Bool(byte == 1)
            }
            TYPE_INT => {
                let value = read_var_i64(buf, &mut cursor)?;
                RawPropValue::Int(value)
            }
            TYPE_FLOAT => {
                if cursor + 8 > buf.len() {
                    return Err(SombraError::Corruption("float payload truncated"));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&buf[cursor..cursor + 8]);
                cursor += 8;
                RawPropValue::Float(f64::from_le_bytes(arr))
            }
            TYPE_STR => decode_bytes_like(buf, &mut cursor, true)?,
            TYPE_BYTES => decode_bytes_like(buf, &mut cursor, false)?,
            TYPE_DATE => {
                let value = read_var_i64(buf, &mut cursor)?;
                RawPropValue::Date(value)
            }
            TYPE_DATETIME => {
                let value = read_var_i64(buf, &mut cursor)?;
                RawPropValue::DateTime(value)
            }
            _ => return Err(SombraError::Corruption("unknown property type tag")),
        };
        props.push(RawProp { prop, value });
    }
    Ok(props)
}

pub fn materialize_props(
    raw: &[RawProp],
    vstore: &VStore,
    tx: &ReadGuard,
) -> Result<Vec<(PropId, PropValueOwned)>> {
    let mut result = Vec::with_capacity(raw.len());
    for prop in raw {
        let value = match &prop.value {
            RawPropValue::Null => PropValueOwned::Null,
            RawPropValue::Bool(v) => PropValueOwned::Bool(*v),
            RawPropValue::Int(v) => PropValueOwned::Int(*v),
            RawPropValue::Float(v) => PropValueOwned::Float(*v),
            RawPropValue::StrInline(bytes) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| SombraError::Corruption("stored string not utf8"))?;
                PropValueOwned::Str(s.to_owned())
            }
            RawPropValue::StrVRef(vref) => {
                let bytes = vstore.read(tx, *vref)?;
                let s = String::from_utf8(bytes)
                    .map_err(|_| SombraError::Corruption("stored string not utf8"))?;
                PropValueOwned::Str(s)
            }
            RawPropValue::BytesInline(bytes) => PropValueOwned::Bytes(bytes.clone()),
            RawPropValue::BytesVRef(vref) => {
                let bytes = vstore.read(tx, *vref)?;
                PropValueOwned::Bytes(bytes)
            }
            RawPropValue::Date(v) => PropValueOwned::Date(*v),
            RawPropValue::DateTime(v) => PropValueOwned::DateTime(*v),
        };
        result.push((prop.prop, value));
    }
    Ok(result)
}

pub fn free_vrefs(vstore: &VStore, tx: &mut WriteGuard<'_>, vrefs: &[VRef]) {
    for vref in vrefs {
        let _ = vstore.free(tx, *vref);
    }
}

fn encode_bytes_like(
    dst: &mut Vec<u8>,
    type_tag: u8,
    bytes: &[u8],
    inline_limit: usize,
    vstore: &VStore,
    tx: &mut WriteGuard<'_>,
    spill_vrefs: &mut Vec<VRef>,
) -> Result<()> {
    dst.push(type_tag);
    if bytes.len() <= inline_limit {
        dst.push(0);
        write_var_u64(bytes.len() as u64, dst);
        dst.extend_from_slice(bytes);
        return Ok(());
    }
    dst.push(1);
    let vref = vstore.write(tx, bytes)?;
    spill_vrefs.push(vref);
    encode_vref(dst, vref);
    Ok(())
}

fn decode_bytes_like(buf: &[u8], cursor: &mut usize, is_string: bool) -> Result<RawPropValue> {
    if *cursor >= buf.len() {
        return Err(SombraError::Corruption("bytes payload truncated"));
    }
    let repr = buf[*cursor];
    *cursor += 1;
    match repr {
        0 => {
            let len = read_var_u64(buf, cursor)? as usize;
            if *cursor + len > buf.len() {
                return Err(SombraError::Corruption("inline bytes truncated"));
            }
            let data = buf[*cursor..*cursor + len].to_vec();
            *cursor += len;
            Ok(if is_string {
                RawPropValue::StrInline(data)
            } else {
                RawPropValue::BytesInline(data)
            })
        }
        1 => {
            if *cursor + 20 > buf.len() {
                return Err(SombraError::Corruption("vref payload truncated"));
            }
            let start_page = read_u64_be(&buf[*cursor..*cursor + 8]);
            *cursor += 8;
            let n_pages = read_u32_be(&buf[*cursor..*cursor + 4]);
            *cursor += 4;
            let len = read_u32_be(&buf[*cursor..*cursor + 4]);
            *cursor += 4;
            let checksum = read_u32_be(&buf[*cursor..*cursor + 4]);
            *cursor += 4;
            let vref = VRef {
                start_page: crate::types::PageId(start_page),
                n_pages,
                len,
                checksum,
            };
            Ok(if is_string {
                RawPropValue::StrVRef(vref)
            } else {
                RawPropValue::BytesVRef(vref)
            })
        }
        _ => Err(SombraError::Corruption("unknown string/bytes repr tag")),
    }
}

fn encode_vref(dst: &mut Vec<u8>, vref: VRef) {
    dst.extend_from_slice(&vref.start_page.0.to_be_bytes());
    dst.extend_from_slice(&vref.n_pages.to_be_bytes());
    dst.extend_from_slice(&vref.len.to_be_bytes());
    dst.extend_from_slice(&vref.checksum.to_be_bytes());
}

fn write_var_u64(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

fn write_var_i64(v: i64, out: &mut Vec<u8>) {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    write_var_u64(zigzag, out);
}

fn read_var_u64(buf: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for _ in 0..10 {
        if *cursor >= buf.len() {
            return Err(SombraError::Corruption("varint truncated"));
        }
        let byte = buf[*cursor];
        *cursor += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(SombraError::Corruption("varint too long"))
}

fn read_var_i64(buf: &[u8], cursor: &mut usize) -> Result<i64> {
    let raw = read_var_u64(buf, cursor)?;
    Ok(((raw >> 1) as i64) ^ (-((raw & 1) as i64)))
}

fn read_u64_be(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(arr)
}

fn read_u32_be(bytes: &[u8]) -> u32 {
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[..4]);
    u32::from_be_bytes(arr)
}
