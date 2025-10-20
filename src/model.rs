use std::cmp::Ordering;
use std::collections::BTreeMap;

pub type NodeId = u64;
pub type EdgeId = u64;

pub const NULL_EDGE_ID: EdgeId = 0;
pub const NULL_NODE_ID: NodeId = 0;

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl PropertyValue {
    pub fn partial_cmp_value(&self, other: &PropertyValue) -> Option<Ordering> {
        match (self, other) {
            (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a.partial_cmp(b),
            (PropertyValue::Int(a), PropertyValue::Int(b)) => a.partial_cmp(b),
            (PropertyValue::Float(a), PropertyValue::Float(b)) => a.partial_cmp(b),
            (PropertyValue::String(a), PropertyValue::String(b)) => a.partial_cmp(b),
            (PropertyValue::Bytes(a), PropertyValue::Bytes(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, PropertyValue>,
    pub first_outgoing_edge_id: EdgeId,
    pub first_incoming_edge_id: EdgeId,
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            labels: Vec::new(),
            properties: BTreeMap::new(),
            first_outgoing_edge_id: NULL_EDGE_ID,
            first_incoming_edge_id: NULL_EDGE_ID,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    pub id: EdgeId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub type_name: String,
    pub properties: BTreeMap<String, PropertyValue>,
    pub next_outgoing_edge_id: EdgeId,
    pub next_incoming_edge_id: EdgeId,
}

impl Edge {
    pub fn new(
        id: EdgeId,
        source_node_id: NodeId,
        target_node_id: NodeId,
        type_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            source_node_id,
            target_node_id,
            type_name: type_name.into(),
            properties: BTreeMap::new(),
            next_outgoing_edge_id: NULL_EDGE_ID,
            next_incoming_edge_id: NULL_EDGE_ID,
        }
    }
}
