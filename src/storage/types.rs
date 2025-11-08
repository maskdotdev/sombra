use std::fmt;

use crate::types::{LabelId, NodeId, PropId, TypeId};

/// Property value with borrowed data (zero-copy).
#[derive(Clone, Debug)]
pub enum PropValue<'a> {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point number.
    Float(f64),
    /// String slice reference.
    Str(&'a str),
    /// Byte slice reference.
    Bytes(&'a [u8]),
    /// Date value represented as Unix timestamp (days since epoch).
    Date(i64),
    /// DateTime value represented as Unix timestamp (milliseconds since epoch).
    DateTime(i64),
}

/// Property entry associating a property ID with a borrowed value.
#[derive(Clone, Debug)]
pub struct PropEntry<'a> {
    /// The property identifier.
    pub prop: PropId,
    /// The property value.
    pub value: PropValue<'a>,
}

/// Property value with owned data.
#[derive(Clone, Debug, PartialEq)]
pub enum PropValueOwned {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point number.
    Float(f64),
    /// Owned string.
    Str(String),
    /// Owned byte vector.
    Bytes(Vec<u8>),
    /// Date value represented as Unix timestamp (days since epoch).
    Date(i64),
    /// DateTime value represented as Unix timestamp (milliseconds since epoch).
    DateTime(i64),
}

impl fmt::Display for PropValueOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropValueOwned::Null => write!(f, "null"),
            PropValueOwned::Bool(v) => write!(f, "{v}"),
            PropValueOwned::Int(v) => write!(f, "{v}"),
            PropValueOwned::Float(v) => write!(f, "{v}"),
            PropValueOwned::Str(v) => write!(f, "{v}"),
            PropValueOwned::Bytes(v) => write!(f, "bytes(len={})", v.len()),
            PropValueOwned::Date(v) => write!(f, "date({v})"),
            PropValueOwned::DateTime(v) => write!(f, "datetime({v})"),
        }
    }
}

impl<'a> PropEntry<'a> {
    /// Creates a new property entry.
    pub fn new(prop: PropId, value: PropValue<'a>) -> Self {
        Self { prop, value }
    }
}

/// Specification for creating a new node with borrowed data.
#[derive(Clone, Debug)]
pub struct NodeSpec<'a> {
    /// Labels to assign to the node.
    pub labels: &'a [LabelId],
    /// Properties to set on the node.
    pub props: &'a [PropEntry<'a>],
}

/// Specification for creating a new edge with borrowed data.
#[derive(Clone, Debug)]
pub struct EdgeSpec<'a> {
    /// Source node ID.
    pub src: NodeId,
    /// Destination node ID.
    pub dst: NodeId,
    /// Edge type ID.
    pub ty: TypeId,
    /// Properties to set on the edge.
    pub props: &'a [PropEntry<'a>],
}

/// Complete node data with owned values.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeData {
    /// Node labels.
    pub labels: Vec<LabelId>,
    /// Node properties as (property ID, value) pairs.
    pub props: Vec<(PropId, PropValueOwned)>,
}

/// Complete edge data with owned values.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeData {
    /// Source node ID.
    pub src: NodeId,
    /// Destination node ID.
    pub dst: NodeId,
    /// Edge type ID.
    pub ty: TypeId,
    /// Edge properties as (property ID, value) pairs.
    pub props: Vec<(PropId, PropValueOwned)>,
}

/// Deletion mode for node deletion operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteMode {
    /// Fails if the node has any connected edges (default).
    Restrict,
    /// Deletes the node and all connected edges.
    Cascade,
}

/// Options for deleting a node from the graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteNodeOpts {
    /// The deletion mode (restrict or cascade).
    pub mode: DeleteMode,
}

impl DeleteNodeOpts {
    /// Creates delete options with restrict mode.
    pub fn restrict() -> Self {
        Self {
            mode: DeleteMode::Restrict,
        }
    }

    /// Creates delete options with cascade mode.
    pub fn cascade() -> Self {
        Self {
            mode: DeleteMode::Cascade,
        }
    }
}

impl Default for DeleteNodeOpts {
    fn default() -> Self {
        Self::restrict()
    }
}
