use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::primitives::pager::{ReadGuard, WriteGuard};
use crate::storage::edge::PropStorage as EdgePropStorage;
use crate::storage::index::TypeTag;
use crate::storage::node::{self, PropStorage as NodePropStorage};
use crate::storage::patch::{PropPatch, PropPatchOp};
use crate::storage::props::{self, RawPropValue};
use crate::storage::types::{PropEntry, PropValue, PropValueOwned};
use crate::types::{LabelId, NodeId, PropId, Result, SombraError, VRef};

use super::{Graph, PropDelta};

pub(crate) fn encode_value_key_owned(ty: TypeTag, value: &PropValueOwned) -> Result<Vec<u8>> {
    match (ty, value) {
        (TypeTag::Null, PropValueOwned::Null) => Ok(Vec::new()),
        (TypeTag::Bool, PropValueOwned::Bool(v)) => Ok(vec![u8::from(*v)]),
        (TypeTag::Int, PropValueOwned::Int(v)) => Ok(encode_i64_key(*v).to_vec()),
        (TypeTag::Float, PropValueOwned::Float(v)) => encode_f64_key(*v),
        (TypeTag::String, PropValueOwned::Str(s)) => encode_bytes_key(s.as_bytes()),
        (TypeTag::Bytes, PropValueOwned::Bytes(b)) => encode_bytes_key(b),
        (TypeTag::Date, PropValueOwned::Date(v)) => Ok(encode_i64_key(*v).to_vec()),
        (TypeTag::DateTime, PropValueOwned::DateTime(v)) => Ok(encode_i64_key(*v).to_vec()),
        _ => Err(SombraError::Invalid(
            "property value type mismatch for index",
        )),
    }
}

pub(crate) fn encode_range_bound(
    ty: TypeTag,
    bound: Bound<&PropValueOwned>,
) -> Result<Bound<Vec<u8>>> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => encode_value_key_owned(ty, value).map(Bound::Included),
        Bound::Excluded(value) => encode_value_key_owned(ty, value).map(Bound::Excluded),
    }
}

pub(crate) fn clone_owned_bound(bound: Bound<&PropValueOwned>) -> Bound<PropValueOwned> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(value.clone()),
        Bound::Excluded(value) => Bound::Excluded(value.clone()),
    }
}

pub(crate) fn prop_value_to_owned(value: PropValue<'_>) -> PropValueOwned {
    match value {
        PropValue::Null => PropValueOwned::Null,
        PropValue::Bool(v) => PropValueOwned::Bool(v),
        PropValue::Int(v) => PropValueOwned::Int(v),
        PropValue::Float(v) => PropValueOwned::Float(v),
        PropValue::Str(v) => PropValueOwned::Str(v.to_owned()),
        PropValue::Bytes(v) => PropValueOwned::Bytes(v.to_vec()),
        PropValue::Date(v) => PropValueOwned::Date(v),
        PropValue::DateTime(v) => PropValueOwned::DateTime(v),
    }
}

pub(crate) fn compare_prop_values(a: &PropValueOwned, b: &PropValueOwned) -> Result<Ordering> {
    use PropValueOwned::*;
    Ok(match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(a), Bool(b)) => a.cmp(b),
        (Int(a), Int(b)) => a.cmp(b),
        (Float(a), Float(b)) => a
            .partial_cmp(b)
            .ok_or(SombraError::Invalid("float comparison invalid"))?,
        (Str(a), Str(b)) => a.cmp(b),
        (Bytes(a), Bytes(b)) => a.cmp(b),
        (Date(a), Date(b)) => a.cmp(b),
        (DateTime(a), DateTime(b)) => a.cmp(b),
        (va, vb) => value_rank(va).cmp(&value_rank(vb)),
    })
}

pub(crate) fn update_min_max(
    slot: &mut Option<PropValueOwned>,
    candidate: &PropValueOwned,
    desired: Ordering,
) -> Result<()> {
    match slot {
        Some(current) => {
            if compare_prop_values(candidate, current)? == desired {
                *slot = Some(candidate.clone());
            }
        }
        None => {
            *slot = Some(candidate.clone());
        }
    }
    Ok(())
}

pub(crate) fn prop_stats_key(value: &PropValueOwned) -> Vec<u8> {
    use PropValueOwned::*;
    let mut out = Vec::new();
    match value {
        Null => out.push(0),
        Bool(v) => {
            out.push(1);
            out.push(u8::from(*v));
        }
        Int(v) => {
            out.push(2);
            out.extend_from_slice(&encode_i64_key(*v));
        }
        Float(v) => {
            out.push(3);
            out.extend_from_slice(&encode_f64_key(*v).unwrap_or_else(|_| vec![0; 8]));
        }
        Str(v) => {
            out.push(4);
            out.extend(encode_bytes_key(v.as_bytes()).unwrap_or_else(|_| v.as_bytes().to_vec()));
        }
        Bytes(v) => {
            out.push(5);
            out.extend(encode_bytes_key(v).unwrap_or_else(|_| v.clone()));
        }
        Date(v) => {
            out.push(6);
            out.extend_from_slice(&encode_i64_key(*v));
        }
        DateTime(v) => {
            out.push(7);
            out.extend_from_slice(&encode_i64_key(*v));
        }
    }
    out
}

fn encode_i64_key(value: i64) -> [u8; 8] {
    ((value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

fn encode_f64_key(value: f64) -> Result<Vec<u8>> {
    if value.is_nan() {
        return Err(SombraError::Invalid("NaN values cannot be indexed"));
    }
    let bits = value.to_bits();
    let normalized = if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    };
    Ok(normalized.to_be_bytes().to_vec())
}

fn encode_bytes_key(bytes: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| SombraError::Invalid("property value exceeds maximum length"))?;
    let mut out = Vec::with_capacity(4 + bytes.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(out)
}

fn value_rank(value: &PropValueOwned) -> u8 {
    use PropValueOwned::*;
    match value {
        Null => 0,
        Bool(_) => 1,
        Int(_) => 2,
        Float(_) => 3,
        Str(_) => 4,
        Bytes(_) => 5,
        Date(_) => 6,
        DateTime(_) => 7,
    }
}

fn apply_patch_ops(map: &mut BTreeMap<PropId, PropValueOwned>, ops: &[PropPatchOp<'_>]) {
    for op in ops {
        match op {
            PropPatchOp::Set(prop, value) => {
                map.insert(*prop, prop_value_to_owned(value.clone()));
            }
            PropPatchOp::Delete(prop) => {
                map.remove(prop);
            }
        }
    }
}

impl Graph {
    pub(crate) fn materialize_raw_prop_value(
        &self,
        tx: &ReadGuard,
        value: &RawPropValue,
    ) -> Result<PropValueOwned> {
        let owned = match value {
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
                let bytes = self.vstore.read(tx, *vref)?;
                let s = String::from_utf8(bytes)
                    .map_err(|_| SombraError::Corruption("stored string not utf8"))?;
                PropValueOwned::Str(s)
            }
            RawPropValue::BytesInline(bytes) => PropValueOwned::Bytes(bytes.clone()),
            RawPropValue::BytesVRef(vref) => {
                let bytes = self.vstore.read(tx, *vref)?;
                PropValueOwned::Bytes(bytes)
            }
            RawPropValue::Date(v) => PropValueOwned::Date(*v),
            RawPropValue::DateTime(v) => PropValueOwned::DateTime(*v),
        };
        Ok(owned)
    }

    pub(crate) fn node_property_value(
        &self,
        tx: &ReadGuard,
        versioned: &node::VersionedNodeRow,
        prop: PropId,
    ) -> Result<Option<PropValueOwned>> {
        let bytes = self.read_node_prop_bytes(&versioned.row.props)?;
        let raw = props::decode_raw(&bytes)?;
        for entry in raw {
            if entry.prop == prop {
                let owned = self.materialize_raw_prop_value(tx, &entry.value)?;
                return Ok(Some(owned));
            }
        }
        Ok(None)
    }

    pub(crate) fn node_matches_property_eq(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        label: LabelId,
        prop: PropId,
        expected: &PropValueOwned,
    ) -> Result<bool> {
        let Some(versioned) = self.visible_node(tx, node)? else {
            return Ok(false);
        };
        if versioned.row.labels.binary_search(&label).is_err() {
            return Ok(false);
        }
        let Some(value) = self.node_property_value(tx, &versioned, prop)? else {
            return Ok(false);
        };
        Ok(value == *expected)
    }

    pub(crate) fn node_matches_property_range(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        label: LabelId,
        prop: PropId,
        start: &Bound<PropValueOwned>,
        end: &Bound<PropValueOwned>,
    ) -> Result<bool> {
        let Some(versioned) = self.visible_node(tx, node)? else {
            return Ok(false);
        };
        if versioned.row.labels.binary_search(&label).is_err() {
            return Ok(false);
        }
        let Some(value) = self.node_property_value(tx, &versioned, prop)? else {
            return Ok(false);
        };
        if !Self::bound_allows(&value, start, true)? {
            return Ok(false);
        }
        if !Self::bound_allows(&value, end, false)? {
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) fn bound_allows(
        value: &PropValueOwned,
        bound: &Bound<PropValueOwned>,
        is_lower: bool,
    ) -> Result<bool> {
        match bound {
            Bound::Unbounded => Ok(true),
            Bound::Included(b) => match compare_prop_values(value, b)? {
                Less if is_lower => Ok(false),
                Greater if !is_lower => Ok(false),
                _ => Ok(true),
            },
            Bound::Excluded(b) => match compare_prop_values(value, b)? {
                Less if is_lower => Ok(false),
                Equal => Ok(!is_lower),
                Greater if !is_lower => Ok(false),
                _ => Ok(true),
            },
        }
    }

    pub(crate) fn encode_property_map(
        &self,
        tx: &mut WriteGuard<'_>,
        props: &[PropEntry<'_>],
    ) -> Result<(Vec<u8>, Vec<VRef>)> {
        let result = props::encode_props(props, self.inline_prop_value, &self.vstore, tx)?;
        Ok((result.bytes, result.spill_vrefs))
    }

    pub(crate) fn free_prop_values_from_bytes(
        &self,
        tx: &mut WriteGuard<'_>,
        bytes: &[u8],
    ) -> Result<()> {
        let raw = props::decode_raw(bytes)?;
        for entry in raw {
            match entry.value {
                RawPropValue::StrVRef(vref) | RawPropValue::BytesVRef(vref) => {
                    self.vstore.free(tx, vref)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn free_node_props(
        &self,
        tx: &mut WriteGuard<'_>,
        props: NodePropStorage,
    ) -> Result<()> {
        match props {
            NodePropStorage::Inline(bytes) => self.free_prop_values_from_bytes(tx, &bytes),
            NodePropStorage::VRef(vref) => {
                let bytes = self.vstore.read_with_write(tx, vref)?;
                self.free_prop_values_from_bytes(tx, &bytes)?;
                self.vstore.free(tx, vref)
            }
        }
    }

    pub(crate) fn read_node_prop_bytes(&self, storage: &NodePropStorage) -> Result<Vec<u8>> {
        match storage {
            NodePropStorage::Inline(bytes) => Ok(bytes.clone()),
            NodePropStorage::VRef(vref) => {
                let read = self.lease_latest_snapshot()?;
                self.vstore.read(&read, *vref)
            }
        }
    }

    pub(crate) fn read_node_prop_bytes_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        storage: &NodePropStorage,
    ) -> Result<Vec<u8>> {
        match storage {
            NodePropStorage::Inline(bytes) => Ok(bytes.clone()),
            NodePropStorage::VRef(vref) => self.vstore.read_with_write(tx, *vref),
        }
    }

    pub(crate) fn read_edge_prop_bytes_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        storage: &EdgePropStorage,
    ) -> Result<Vec<u8>> {
        match storage {
            EdgePropStorage::Inline(bytes) => Ok(bytes.clone()),
            EdgePropStorage::VRef(vref) => self.vstore.read_with_write(tx, *vref),
        }
    }

    pub(crate) fn materialize_props_owned(
        &self,
        bytes: &[u8],
    ) -> Result<Vec<(PropId, PropValueOwned)>> {
        let raw = props::decode_raw(bytes)?;
        let read = self.lease_latest_snapshot()?;
        let props = props::materialize_props(&raw, &self.vstore, &read)?;
        Ok(props)
    }

    pub(crate) fn materialize_props_owned_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        bytes: &[u8],
    ) -> Result<Vec<(PropId, PropValueOwned)>> {
        let raw = props::decode_raw(bytes)?;
        props::materialize_props_with_write(&raw, &self.vstore, tx)
    }

    pub(crate) fn build_prop_delta(
        &self,
        tx: &mut WriteGuard<'_>,
        prop_bytes: &[u8],
        patch: &PropPatch<'_>,
    ) -> Result<Option<PropDelta>> {
        if patch.is_empty() {
            return Ok(None);
        }
        let current = self.materialize_props_owned_with_write(tx, prop_bytes)?;
        let mut new_map: BTreeMap<PropId, PropValueOwned> = current.into_iter().collect();
        let old_map = new_map.clone();
        apply_patch_ops(&mut new_map, &patch.ops);
        if new_map == old_map {
            return Ok(None);
        }
        let ordered = new_map
            .iter()
            .map(|(prop, value)| (*prop, value.clone()))
            .collect::<Vec<_>>();
        let encoded =
            props::encode_props_owned(&ordered, self.inline_prop_value, &self.vstore, tx)?;
        Ok(Some(PropDelta {
            old_map,
            new_map,
            encoded,
        }))
    }
}
