use std::fmt;

use sombra_types::{LabelId, NodeId, PropId, TypeId};

#[derive(Clone, Debug)]
pub enum PropValue<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a str),
    Bytes(&'a [u8]),
    Date(i64),
    DateTime(i64),
}

#[derive(Clone, Debug)]
pub struct PropEntry<'a> {
    pub prop: PropId,
    pub value: PropValue<'a>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PropValueOwned {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Date(i64),
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
    pub fn new(prop: PropId, value: PropValue<'a>) -> Self {
        Self { prop, value }
    }
}

#[derive(Clone, Debug)]
pub struct NodeSpec<'a> {
    pub labels: &'a [LabelId],
    pub props: &'a [PropEntry<'a>],
}

#[derive(Clone, Debug)]
pub struct EdgeSpec<'a> {
    pub src: NodeId,
    pub dst: NodeId,
    pub ty: TypeId,
    pub props: &'a [PropEntry<'a>],
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeData {
    pub labels: Vec<LabelId>,
    pub props: Vec<(PropId, PropValueOwned)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EdgeData {
    pub src: NodeId,
    pub dst: NodeId,
    pub ty: TypeId,
    pub props: Vec<(PropId, PropValueOwned)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteMode {
    Restrict,
    Cascade,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteNodeOpts {
    pub mode: DeleteMode,
}

impl DeleteNodeOpts {
    pub fn restrict() -> Self {
        Self {
            mode: DeleteMode::Restrict,
        }
    }

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
